# %% [markdown]
# # nb_03_silver_facts — clean, type, dedupe, conform
#
# | Silver table | Source | Technique |
# |---|---|---|
# | `silver_order_line` | mirrored orders + order_items | **incremental** load by `updated_at` watermark, MERGE upsert |
# | `silver_pos_event` | `bronze_pos_events` (Eventstream) | **Structured Streaming** + `foreachBatch` MERGE ⇒ exactly-once, dedupe on `event_id` |
# | `silver_fx_rate_daily` | `bronze_fx_rates` | gap-fill weekends/holidays (last known rate) |
# | `silver_inventory_snapshot` | `bronze_inventory` | type + quarantine negative stock |
# | `silver_review` | `bronze_reviews` (raw JSON) | parse nested JSON, flatten, dedupe, quarantine bad ratings |
# | `silver_support_case` | `bronze_crm_cases` | type timestamps, derive resolution hours |

# %% tags=["parameters"]
pipeline_run_id = "interactive"

# %%
# MAGIC %run nb_00_common

# %% [markdown]
# ## 1. Online order lines — incremental with a watermark

# %%
wm = get_watermark(spark, "silver_order_line")
# ">=" (not ">") so rows sharing the boundary timestamp are never skipped; MERGE makes re-reads harmless
orders = spark.table(TBL["src_orders"]).filter(F.col("updated_at") >= F.lit(wm).cast("timestamp"))
new_wm = orders.agg(F.max("updated_at")).collect()[0][0]

lines = (orders.join(spark.table(TBL["src_order_items"]), "order_id")
         .select(F.col("order_id").cast("int"), F.col("line_no").cast("int"),
                 F.col("customer_id").cast("int"), "product_id",
                 F.col("order_ts").cast("timestamp"),
                 F.to_date("order_ts").alias("order_date"),
                 "channel", "currency", "order_status", "payment_method",
                 F.col("quantity").cast("int"),
                 F.col("unit_price").cast("decimal(12,2)").alias("unit_price_local"),
                 F.col("discount_pct").cast("decimal(5,2)"),
                 F.col("updated_at").cast("timestamp").alias("source_updated_at"))
         .withColumn("gross_amount_local", (F.col("quantity") * F.col("unit_price_local")).cast("decimal(14,2)"))
         .withColumn("net_amount_local",
                     (F.col("gross_amount_local") * (1 - F.col("discount_pct") / 100)).cast("decimal(14,2)")))

if new_wm is not None:
    m = merge_upsert(spark, lines, TBL["silver_order_line"], ["order_id", "line_no"],
                     update_condition="s.source_updated_at > t.source_updated_at")
    set_watermark(spark, "silver_order_line", new_wm)
else:
    m = {"inserted": 0, "updated": 0, "note": "no new/changed orders"}
log_run(spark, "nb_03_silver_facts", TBL["silver_order_line"], m)

# %% [markdown]
# ## 2. POS events — streaming, exactly-once into Silver
# `skipChangeCommits` ignores maintenance commits (e.g. OPTIMIZE) on the Eventstream table.
# `foreachBatch` + MERGE on `event_id` makes retries and duplicate deliveries harmless.

# %%
def upsert_pos(batch_df, batch_id):
    clean = (batch_df
             .select("event_id", "event_type", F.to_timestamp("event_ts").alias("event_ts"),
                     "basket_id", F.col("line_no").cast("int"), "store_id", "register_id",
                     F.col("customer_id").cast("int"), "product_id",
                     F.col("quantity").cast("int"),
                     F.col("unit_price").cast("decimal(12,2)").alias("unit_price_local"),
                     F.col("line_amount").cast("decimal(14,2)").alias("net_amount_local"),
                     "currency", "payment_method")
             .filter("event_id IS NOT NULL AND event_ts IS NOT NULL")
             .dropDuplicates(["event_id"])
             .withColumn("event_date", F.to_date("event_ts"))
             .withColumn("_updated_at", F.current_timestamp()))
    target = TBL["silver_pos"]
    if not spark.catalog.tableExists(target):
        clean.write.format("delta").partitionBy("event_date").saveAsTable(target)
        return
    (DeltaTable.forName(spark, target).alias("t")
        .merge(clean.alias("s"), "t.event_id = s.event_id AND t.event_date = s.event_date")
        .whenNotMatchedInsertAll()
        .execute())


if table_exists(spark, TBL["bronze_pos"]):
    q = (spark.readStream.option("skipChangeCommits", "true").table(TBL["bronze_pos"])
         .writeStream.foreachBatch(upsert_pos)
         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_pos_event")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    log_run(spark, "nb_03_silver_facts", TBL["silver_pos"],
            {"rows_read": sum(p.get("numInputRows", 0) for p in q.recentProgress)})
else:
    print("bronze_pos_events not found yet — start the Eventstream first. Skipping.")

# %% [markdown]
# ## 3. FX rates — typed + gap-filled calendar
# The API has no rates for weekends/holidays. Sales still happen on those days, so we
# carry the last known rate forward. Rates are quoted "1 INR = x CCY"; we store the
# inverse, `inr_per_unit`, so Gold can do `amount_local * inr_per_unit`.

# %%
fx = (spark.table(TBL["bronze_fx"])
      .select(F.to_date("date").alias("rate_date"), F.upper("quote").alias("currency"),
              F.col("rate").cast("double").alias("rate"))
      .filter("rate IS NOT NULL AND rate > 0")
      .dropDuplicates(["rate_date", "currency"]))

bounds = fx.agg(F.min("rate_date").alias("lo")).collect()[0]
calendar = spark.sql(f"SELECT explode(sequence(DATE'{bounds.lo}', current_date(), INTERVAL 1 DAY)) AS rate_date")
ccys = fx.select("currency").distinct()
w = Window.partitionBy("currency").orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
fx_daily = (calendar.crossJoin(ccys).join(fx, ["rate_date", "currency"], "left")
            .withColumn("is_filled", F.col("rate").isNull())
            .withColumn("rate", F.last("rate", ignorenulls=True).over(w))
            .withColumn("inr_per_unit", F.round(1 / F.col("rate"), 6))
            .select("rate_date", "currency", "inr_per_unit", "is_filled"))
inr = calendar.select("rate_date", F.lit("INR").alias("currency"),
                      F.lit(1.0).alias("inr_per_unit"), F.lit(False).alias("is_filled"))
fx_daily = fx_daily.unionByName(inr)
fx_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TBL["silver_fx"])
log_run(spark, "nb_03_silver_facts", TBL["silver_fx"], {"rows": fx_daily.count()})

# %% [markdown]
# ## 4. Inventory snapshots

# %%
inv = (spark.table(TBL["bronze_inventory"])
       .select(F.to_date("snapshot_date").alias("snapshot_date"), "store_id",
               F.col("sku").alias("product_id"),
               F.col("on_hand_qty").cast("int"), F.col("reorder_point").cast("int"),
               F.to_date(F.when(F.col("last_restock_date") != "", F.col("last_restock_date"))).alias("last_restock_date"),
               "_ingested_at")
       .withColumn("_rn", F.row_number().over(
           Window.partitionBy("snapshot_date", "store_id", "product_id").orderBy(F.col("_ingested_at").desc())))
       .filter("_rn = 1").drop("_rn", "_ingested_at")
       .withColumn("_dq_reason",
                   F.when(F.col("on_hand_qty").isNull(), "on_hand_qty not numeric")
                    .when(F.col("on_hand_qty") < 0, "negative stock")
                    .when(F.col("snapshot_date").isNull(), "bad snapshot_date")))
n_bad = quarantine(spark, inv.filter("_dq_reason IS NOT NULL"), TBL["silver_inventory"])
good = inv.filter("_dq_reason IS NULL").drop("_dq_reason")
m = merge_upsert(spark, good, TBL["silver_inventory"], ["snapshot_date", "store_id", "product_id"])
m["quarantined"] = n_bad
log_run(spark, "nb_03_silver_facts", TBL["silver_inventory"], m)

# %% [markdown]
# ## 5. Reviews — parse nested JSON, flatten, dedupe, quarantine

# %%
review_schema = T.StructType([
    T.StructField("review_id", T.StringType()),
    T.StructField("product", T.StructType([T.StructField("sku", T.StringType())])),
    T.StructField("customer", T.StructType([T.StructField("id", T.IntegerType()),
                                            T.StructField("email", T.StringType())])),
    T.StructField("rating", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("body", T.StringType()),
    T.StructField("submitted_at", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("helpful_votes", T.IntegerType()),
])
rev = (spark.table(TBL["bronze_reviews"])
       .withColumn("j", F.from_json("raw_json", review_schema))
       .select(F.col("j.review_id").alias("review_id"),
               F.col("j.product.sku").alias("product_id"),
               F.col("j.customer.id").alias("customer_id"),
               F.col("j.rating").alias("rating"),
               F.col("j.title").alias("title"),
               F.col("j.body").alias("body"),
               F.to_timestamp("j.submitted_at").alias("submitted_at"),
               F.col("j.source").alias("source"),
               F.col("j.helpful_votes").alias("helpful_votes"),
               "raw_json")
       .dropDuplicates(["review_id"])
       .withColumn("_dq_reason",
                   F.when(F.col("review_id").isNull(), "unparseable json")
                    .when(~F.col("rating").between(1, 5), "rating out of range")
                    .when(F.col("product_id").isNull(), "missing product sku")))
n_bad = quarantine(spark, rev.filter("_dq_reason IS NOT NULL"), TBL["silver_review"])
good = rev.filter("_dq_reason IS NULL").drop("_dq_reason", "raw_json")
m = merge_upsert(spark, good, TBL["silver_review"], ["review_id"])
m["quarantined"] = n_bad
log_run(spark, "nb_03_silver_facts", TBL["silver_review"], m)

# %% [markdown]
# ## 6. Support cases (Salesforce)

# %%
cases = (spark.table(TBL["bronze_cases"])
         .select(F.col("Legacy_Case_Id__c").alias("case_id"),
                 F.col("Legacy_Customer_Id__c").cast("int").alias("customer_id"),
                 F.lower("SuppliedEmail").alias("email"),
                 F.col("Subject").alias("subject"),
                 F.col("Status").alias("status"),
                 F.col("Priority").alias("priority"),
                 F.col("Origin").alias("origin"),
                 F.col("Reason").alias("reason"),
                 F.to_timestamp("Opened_At__c").alias("opened_at"),
                 F.to_timestamp(F.when(F.col("Resolved_At__c") != "", F.col("Resolved_At__c"))).alias("resolved_at"),
                 F.col("CSAT_Score__c").cast("double").cast("int").alias("csat_score"),
                 "_ingested_at")
         .withColumn("_rn", F.row_number().over(Window.partitionBy("case_id").orderBy(F.col("_ingested_at").desc())))
         .filter("_rn = 1 AND case_id IS NOT NULL").drop("_rn", "_ingested_at")
         .withColumn("is_open", F.col("status") != "Closed")
         .withColumn("resolution_hours",
                     F.round((F.unix_timestamp("resolved_at") - F.unix_timestamp("opened_at")) / 3600, 1)))
# customer_id missing? fall back to email lookup against current customers
cur_cust = spark.table(TBL["silver_customer"]).filter("is_current").select(
    F.col("customer_id").alias("_cid"), F.col("email").alias("_email"))
cases = (cases.join(cur_cust, cases.email == cur_cust._email, "left")
              .withColumn("customer_id", F.coalesce("customer_id", "_cid")).drop("_cid", "_email"))
m = merge_upsert(spark, cases, TBL["silver_case"], ["case_id"])
log_run(spark, "nb_03_silver_facts", TBL["silver_case"], m)
