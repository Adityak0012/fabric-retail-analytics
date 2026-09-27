# %% [markdown]
# # nb_00_common — shared config & helpers
# Every other notebook starts with `%run nb_00_common`.
# Attach the **lh_retail** lakehouse as the default lakehouse on every notebook.
#
# Contents
# * table-name config (one place to rename things)
# * `add_audit_columns`, `merge_upsert` (SCD1), `scd2_merge` (SCD Type 2)
# * watermarks for incremental loads, run logging, quarantine writer

# %%
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# All timestamps are handled in UTC; reports convert to local time at the edge.
spark.conf.set("spark.sql.session.timeZone", "UTC")
# SCD2 open/close sentinels (1900-01-01 / 9999-12-31) are "ancient" dates for Parquet;
# write them as-is instead of failing on legacy-calendar rebasing.
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

HIGH_DATE = "9999-12-31 00:00:00"
LOW_DATE = "1900-01-01 00:00:00"
CHECKPOINT_ROOT = "Files/_checkpoints"
LANDING_ROOT = "Files/landing"

TBL = {
    # sources exposed as OneLake shortcuts to the mirrored SQL Server DB
    "src_customers": "src_customers",
    "src_products": "src_products",
    "src_stores": "src_stores",
    "src_orders": "src_orders",
    "src_order_items": "src_order_items",
    # bronze
    "bronze_pos": "bronze_pos_events",          # written by Eventstream
    "bronze_fx": "bronze_fx_rates",
    "bronze_inventory": "bronze_inventory",
    "bronze_reviews": "bronze_reviews",
    "bronze_cases": "bronze_crm_cases",
    # silver
    "silver_customer": "silver_customer",       # SCD2
    "silver_product": "silver_product",         # SCD2
    "silver_store": "silver_store",
    "silver_order_line": "silver_order_line",
    "silver_pos": "silver_pos_event",
    "silver_fx": "silver_fx_rate_daily",
    "silver_inventory": "silver_inventory_snapshot",
    "silver_review": "silver_review",
    "silver_case": "silver_support_case",
    "quarantine": "silver_quarantine",
    # gold
    "dim_date": "gold_dim_date",
    "dim_customer": "gold_dim_customer",
    "dim_product": "gold_dim_product",
    "dim_store": "gold_dim_store",
    "fact_sales": "gold_fact_sales",
    "fact_inventory": "gold_fact_inventory_daily",
    "fact_case": "gold_fact_support_case",
    "fact_review": "gold_fact_review",
    "churn_scores": "gold_churn_scores",
    # ops
    "dq_results": "ops_dq_results",
    "run_log": "ops_run_log",
    "watermark": "ops_watermark",
}


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def table_exists(spark, name: str) -> bool:
    return spark.catalog.tableExists(name)


def add_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return (df.withColumn("_source", F.lit(source))
              .withColumn("_ingested_at", F.current_timestamp()))


def row_hash(cols):
    """Stable hash of the tracked attributes — change detection for SCD2."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols]), 256)


# %% [markdown]
# ## SCD Type 1 upsert

# %%
def merge_upsert(spark, df: DataFrame, target: str, keys: list, update_condition: str | None = None):
    """Insert new rows, overwrite changed rows (no history). Creates the table on first run."""
    df = df.withColumn("_updated_at", F.current_timestamp())
    if not table_exists(spark, target):
        df.write.format("delta").saveAsTable(target)
        return {"inserted": df.count(), "updated": 0}
    cond = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
    m = (DeltaTable.forName(spark, target).alias("t")
         .merge(df.alias("s"), cond)
         .whenMatchedUpdateAll(condition=update_condition)
         .whenNotMatchedInsertAll())
    m.execute()
    return last_merge_metrics(spark, target)


def last_merge_metrics(spark, target: str) -> dict:
    h = DeltaTable.forName(spark, target).history(1).collect()[0]
    m = h["operationMetrics"] or {}
    return {"inserted": int(m.get("numTargetRowsInserted", 0)),
            "updated": int(m.get("numTargetRowsUpdated", 0))}


# %% [markdown]
# ## SCD Type 2 merge
#
# Target layout: `<sk_col>` surrogate key, business key(s), attributes,
# `row_hash`, `valid_from`, `valid_to` (9999-12-31 for current),
# `is_current`, `is_deleted`, `_updated_at`.
#
# Algorithm (single atomic Delta MERGE, the classic "staged union" pattern):
# 1. Keep the latest source row per business key and hash the tracked columns.
# 2. Stage = every source row keyed by its business key **UNION** a second copy of
#    each *changed* row with a NULL merge key.
# 3. MERGE on business key + `is_current`:
#    * matched & hash differs → close the old version (`valid_to = effective ts`)
#    * not matched (new key, or the NULL-keyed copy) → insert new current version
#    * (optional) current target rows missing from the source → close + flag deleted
#
# First load: versions start at 1900-01-01 so facts older than the first load still
# find a dimension row.

# %%
def scd2_merge(spark, src: DataFrame, target: str, keys: list, tracked: list,
               sk_col: str, effective_col: str | None = None, handle_deletes: bool = False) -> dict:
    eff = F.col(effective_col).cast("timestamp") if effective_col else F.current_timestamp()
    w = Window.partitionBy(*keys).orderBy(eff.desc())
    src = (src.withColumn("_eff_ts", eff)
              .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
              .withColumn("_bk", F.concat_ws("|", *[F.col(k).cast("string") for k in keys]))
              .withColumn("row_hash", row_hash(tracked)))
    attr_cols = [c for c in src.columns if c not in ("_eff_ts", "_bk", "row_hash")]

    if not table_exists(spark, target):
        first = (src.withColumn(sk_col, F.row_number().over(Window.orderBy("_bk")).cast("long"))
                    .withColumn("valid_from", F.lit(LOW_DATE).cast("timestamp"))
                    .withColumn("valid_to", F.lit(HIGH_DATE).cast("timestamp"))
                    .withColumn("is_current", F.lit(True))
                    .withColumn("is_deleted", F.lit(False))
                    .withColumn("_updated_at", F.current_timestamp())
                    .select(sk_col, "_bk", *attr_cols, "row_hash", "valid_from", "valid_to",
                            "is_current", "is_deleted", "_updated_at"))
        first.write.format("delta").saveAsTable(target)
        return {"inserted": first.count(), "expired": 0, "deleted": 0}

    tgt = DeltaTable.forName(spark, target)
    current = spark.table(target).filter("is_current").select("_bk", F.col("row_hash").alias("_t_hash"))
    changed = (src.join(current, "_bk", "inner")
                  .filter(F.col("row_hash") != F.col("_t_hash")).drop("_t_hash"))
    staged = (src.withColumn("_merge_key", F.col("_bk"))
                 .unionByName(changed.withColumn("_merge_key", F.lit(None).cast("string"))))
    max_sk = spark.table(target).agg(F.max(sk_col)).collect()[0][0] or 0
    staged = staged.withColumn(sk_col, (F.lit(max_sk) + F.row_number().over(Window.orderBy("_bk", "_merge_key"))).cast("long"))

    insert_vals = {sk_col: f"s.{sk_col}", "_bk": "s._bk", "row_hash": "s.row_hash",
                   "valid_from": "s._eff_ts", "valid_to": f"CAST('{HIGH_DATE}' AS TIMESTAMP)",
                   "is_current": "true", "is_deleted": "false", "_updated_at": "current_timestamp()"}
    insert_vals.update({c: f"s.`{c}`" for c in attr_cols})

    merge = (tgt.alias("t")
             .merge(staged.alias("s"), "t._bk = s._merge_key AND t.is_current = true")
             .whenMatchedUpdate(condition="t.row_hash <> s.row_hash",
                                set={"is_current": "false", "valid_to": "s._eff_ts",
                                     "_updated_at": "current_timestamp()"})
             .whenNotMatchedInsert(values=insert_vals))
    if handle_deletes:
        merge = merge.whenNotMatchedBySourceUpdate(
            condition="t.is_current = true",
            set={"is_current": "false", "is_deleted": "true",
                 "valid_to": "current_timestamp()", "_updated_at": "current_timestamp()"})
    merge.execute()
    m = tgt.history(1).collect()[0]["operationMetrics"] or {}
    return {"inserted": int(m.get("numTargetRowsInserted", 0)),
            "expired": int(m.get("numTargetRowsUpdated", 0)),
            "deleted": int(m.get("numTargetRowsNotMatchedBySourceUpdated", 0) or 0)}


# %% [markdown]
# ## Watermarks, run log, quarantine

# %%
def get_watermark(spark, name: str, default: str = LOW_DATE):
    if not table_exists(spark, TBL["watermark"]):
        return default
    r = spark.table(TBL["watermark"]).filter(F.col("name") == name).select("value").collect()
    return r[0][0] if r else default


def set_watermark(spark, name: str, value) -> None:
    df = spark.createDataFrame([(name, str(value))], "name string, value string")
    merge_upsert(spark, df, TBL["watermark"], ["name"])


def log_run(spark, notebook: str, target: str, metrics: dict, status: str = "succeeded") -> None:
    run_id = globals().get("pipeline_run_id", "interactive")
    row = [(run_id, notebook, target, status, str(metrics), now_utc())]
    (spark.createDataFrame(row, "run_id string, notebook string, target string, status string, "
                                "metrics string, logged_at timestamp")
          .write.format("delta").mode("append").saveAsTable(TBL["run_log"]))
    print(f"[{status}] {notebook} -> {target}: {metrics}")


def quarantine(spark, bad: DataFrame, source_table: str, reason_col: str = "_dq_reason") -> int:
    """Park rows that fail hard rules; keep them as JSON so any schema fits one table."""
    n = bad.count()
    if n:
        rows = (bad.select(F.lit(source_table).alias("source_table"),
                           F.col(reason_col).alias("reason"),
                           F.to_json(F.struct(*[c for c in bad.columns if c != reason_col])).alias("record"))
                   .withColumn("record_hash", F.sha2("record", 256))
                   .dropDuplicates(["source_table", "record_hash"])
                   .withColumn("quarantined_at", F.current_timestamp()))
        if not table_exists(spark, TBL["quarantine"]):
            rows.write.format("delta").saveAsTable(TBL["quarantine"])
        else:   # insert-only merge: re-processing the same bad row never duplicates it
            (DeltaTable.forName(spark, TBL["quarantine"]).alias("t")
                .merge(rows.alias("s"), "t.source_table = s.source_table AND t.record_hash = s.record_hash")
                .whenNotMatchedInsertAll().execute())
    return n

print("nb_00_common loaded")
