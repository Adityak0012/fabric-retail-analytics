# %% [markdown]
# # nb_01_bronze_ingest — landing files ➜ Bronze Delta tables
#
# **Schema-on-read**: Bronze keeps every value as a string (JSON kept as raw text),
# plus lineage columns (`_source_file`, `_ingested_at`). Parsing & typing happen in Silver,
# so a bad upstream file never breaks ingestion.
#
# **Incremental by design**: each source is a Structured Streaming query with
# `trigger(availableNow=True)` + a checkpoint. Spark remembers which files it has already
# processed, so re-running only picks up new files — batch cost, streaming bookkeeping.
#
# | Landing folder (Files/landing/…) | Written by | Bronze table |
# |---|---|---|
# | `fx/` (JSON array) | Pipeline Copy activity from REST API | `bronze_fx_rates` |
# | `inventory/` (CSV) | Pipeline Copy activity from Blob | `bronze_inventory` |
# | `reviews/` (JSON lines) | Pipeline Copy activity from Blob | `bronze_reviews` |
# | `crm/` (CSV) | Copy activity from Salesforce (or Blob fallback) | `bronze_crm_cases` |
#
# POS events don't pass through here — Eventstream writes `bronze_pos_events` directly.
# SQL Server tables don't either — Mirroring keeps `src_*` shortcuts in sync.

# %% tags=["parameters"]
pipeline_run_id = "interactive"

# %%
# MAGIC %run nb_00_common

# %%
def string_schema(cols):
    return T.StructType([T.StructField(c, T.StringType(), True) for c in cols])


SOURCES = [
    {
        "name": "fx", "path": f"{LANDING_ROOT}/fx/", "target": TBL["bronze_fx"], "format": "json",
        "options": {"multiLine": "true"},
        "schema": string_schema(["date", "base", "quote", "rate"]),
    },
    {
        "name": "inventory", "path": f"{LANDING_ROOT}/inventory/", "target": TBL["bronze_inventory"],
        "format": "csv", "options": {"header": "true"},
        "schema": string_schema(["snapshot_date", "store_id", "sku", "on_hand_qty",
                                 "reorder_point", "last_restock_date"]),
    },
    {
        # keep each JSON line untouched; Silver parses it
        "name": "reviews", "path": f"{LANDING_ROOT}/reviews/", "target": TBL["bronze_reviews"],
        "format": "text", "options": {}, "schema": None,
    },
    {
        "name": "crm", "path": f"{LANDING_ROOT}/crm/", "target": TBL["bronze_cases"], "format": "csv",
        "options": {"header": "true", "multiLine": "true", "escape": '"'},
        "schema": string_schema(["Legacy_Case_Id__c", "Legacy_Customer_Id__c", "SuppliedEmail",
                                 "Subject", "Description", "Status", "Priority", "Origin", "Reason",
                                 "Opened_At__c", "Resolved_At__c", "CSAT_Score__c"]),
    },
]


def ingest(src: dict) -> dict:
    reader = spark.readStream.format(src["format"]).options(**src["options"])
    if src["schema"] is not None:
        reader = reader.schema(src["schema"])
    df = reader.load(src["path"])
    if src["format"] == "text":
        df = df.withColumnRenamed("value", "raw_json")
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_pipeline_run_id", F.lit(pipeline_run_id)))
    q = (df.writeStream.format("delta")
           .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_{src['name']}")
           .option("mergeSchema", "true")
           .trigger(availableNow=True)
           .toTable(src["target"]))
    q.awaitTermination()
    rows = sum((p.get("numInputRows", 0) for p in q.recentProgress), 0)
    return {"rows": rows}


# %%
from notebookutils import mssparkutils  # noqa: E402  (available in every Fabric notebook)

for src in SOURCES:
    try:
        mssparkutils.fs.mkdirs(src["path"])          # first run: folder may not exist yet
        metrics = ingest(src)
        log_run(spark, "nb_01_bronze_ingest", src["target"], metrics)
    except Exception as e:                              # log, then fail the pipeline activity
        log_run(spark, "nb_01_bronze_ingest", src["target"], {"error": str(e)[:500]}, "failed")
        raise

# %%
display(spark.sql(f"SELECT '{TBL['bronze_fx']}' t, count(*) n FROM {TBL['bronze_fx']} UNION ALL "
                  f"SELECT '{TBL['bronze_inventory']}', count(*) FROM {TBL['bronze_inventory']} UNION ALL "
                  f"SELECT '{TBL['bronze_reviews']}', count(*) FROM {TBL['bronze_reviews']} UNION ALL "
                  f"SELECT '{TBL['bronze_cases']}', count(*) FROM {TBL['bronze_cases']}"))
