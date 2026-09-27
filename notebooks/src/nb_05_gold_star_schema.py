# %% [markdown]
# # nb_05_gold_star_schema — Silver ➜ Gold star schema
#
# ```
#                 gold_dim_date
#                      │
# gold_dim_customer ─ gold_fact_sales ─ gold_dim_product
#                      │
#                 gold_dim_store
# ```
# Plus `gold_fact_inventory_daily`, `gold_fact_support_case`, `gold_fact_review`.
#
# Key ideas
# * **Surrogate keys** (`*_sk`) from the SCD2 Silver tables; `-1` = "Unknown" member so
#   facts never drop rows because of a missing dimension.
# * **Point-in-time lookups**: each sale joins the customer/product *version that was
#   valid at the time of the sale* (`valid_from <= ts < valid_to`). A customer who moved
#   from Pune to Mumbai keeps old sales attributed to Pune.
# * **One fact for all channels**: online orders (web/app) and in-store POS events are
#   conformed to one grain (one row per sales line) with a `channel` column.
# * **Currency conformance**: every amount also stored in INR using that day's FX rate.

# %% tags=["parameters"]
pipeline_run_id = "interactive"

# %%
# MAGIC %run nb_00_common

# %%
def overwrite(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    n = spark.table(name).count()
    log_run(spark, "nb_05_gold_star_schema", name, {"rows": n})


def with_unknown(df, sk_col, defaults: dict):
    """Append the -1 'Unknown' member row."""
    unknown = spark.createDataFrame([{**{c: None for c in df.columns}, sk_col: -1, **defaults}], df.schema)
    return df.unionByName(unknown)

# %% [markdown]
# ## Dimensions

# %%
# ---- dim_date (Indian fiscal year starts 1 April) ---------------------------
dim_date = (spark.sql("SELECT explode(sequence(DATE'2022-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date")
            .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                    F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                    F.month("date").alias("month"), F.date_format("date", "MMM").alias("month_name"),
                    F.date_format("date", "yyyy-MM").alias("year_month"),
                    F.dayofmonth("date").alias("day"), F.date_format("date", "E").alias("weekday_name"),
                    F.dayofweek("date").alias("weekday_num"),
                    F.dayofweek("date").isin(1, 7).alias("is_weekend"),
                    F.when(F.month("date") >= 4, F.year("date") + 1).otherwise(F.year("date")).alias("fiscal_year"),
                    ((F.month("date") + 8) % 12 / 3 + 1).cast("int").alias("fiscal_quarter")))
overwrite(dim_date, TBL["dim_date"])

# ---- dim_customer (SCD2 passthrough, no PII beyond what reports need) -------
dim_customer = (spark.table(TBL["silver_customer"])
                .select("customer_sk", "customer_id",
                        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
                        "email", "city", "state", "country", "loyalty_tier", "marketing_opt_in",
                        "signup_date", "valid_from", "valid_to", "is_current", "is_deleted"))
dim_customer = with_unknown(dim_customer, "customer_sk",
                            {"customer_name": "Unknown / walk-in", "customer_id": -1,
                             "valid_from": datetime(1900, 1, 1), "valid_to": datetime(9999, 12, 31),
                             "is_current": True, "is_deleted": False})
overwrite(dim_customer, TBL["dim_customer"])

# ---- dim_product (SCD2) ----------------------------------------------------
dim_product = (spark.table(TBL["silver_product"])
               .select("product_sk", "product_id", "product_name", "category", "brand",
                       "unit_price_inr", "unit_cost_inr", "is_active",
                       "valid_from", "valid_to", "is_current"))
dim_product = with_unknown(dim_product, "product_sk",
                           {"product_name": "Unknown", "product_id": "N/A", "category": "Unknown",
                            "valid_from": datetime(1900, 1, 1), "valid_to": datetime(9999, 12, 31),
                            "is_current": True})
overwrite(dim_product, TBL["dim_product"])

# ---- dim_store (SCD1) + a virtual ONLINE store for web/app orders ----------
stores = spark.table(TBL["silver_store"]).select("store_id", "store_name", "city", "state", "country",
                                                  "currency", "store_format", "opened_date")
online = spark.createDataFrame([("ONLINE", "Online (web + app)", None, None, None, None, "Online", None)],
                               stores.schema)
dim_store = (stores.unionByName(online)
             .withColumn("store_sk", F.dense_rank().over(Window.orderBy("store_id")).cast("int")))
dim_store = with_unknown(dim_store, "store_sk", {"store_id": "N/A", "store_name": "Unknown"})
overwrite(dim_store, TBL["dim_store"])

# %% [markdown]
# ## fact_sales — online + in-store, one grain

# %%
online_lines = (spark.table(TBL["silver_order_line"])
                .select(F.concat(F.lit("O-"), "order_id", F.lit("-"), "line_no").alias("sales_line_id"),
                        F.col("order_id").cast("string").alias("transaction_id"),
                        F.col("order_ts").alias("txn_ts"), "customer_id", "product_id",
                        F.lit("ONLINE").alias("store_id"), "channel",
                        F.when(F.col("order_status") == "completed", "sale")
                         .when(F.col("order_status") == "returned", "return")
                         .otherwise("cancelled").alias("transaction_type"),
                        "quantity", "unit_price_local", "discount_pct", "currency", "payment_method",
                        "gross_amount_local",
                        # revenue recognised: completed = net; returned/cancelled = 0
                        F.when(F.col("order_status") == "completed", F.col("net_amount_local"))
                         .otherwise(F.lit(0)).cast("decimal(14,2)").alias("net_amount_local")))

pos_lines = spark.createDataFrame([], online_lines.schema)
if table_exists(spark, TBL["silver_pos"]):
    pos_lines = (spark.table(TBL["silver_pos"])
                 .select(F.concat(F.lit("P-"), "event_id").alias("sales_line_id"),
                         F.col("basket_id").alias("transaction_id"),
                         F.col("event_ts").alias("txn_ts"), "customer_id", "product_id", "store_id",
                         F.lit("in_store").alias("channel"),
                         F.col("event_type").alias("transaction_type"),
                         "quantity", "unit_price_local", F.lit(0).cast("decimal(5,2)").alias("discount_pct"),
                         "currency", "payment_method",
                         F.abs("net_amount_local").cast("decimal(14,2)").alias("gross_amount_local"),
                         "net_amount_local"))           # POS returns are already negative

sales = online_lines.unionByName(pos_lines)

cust = spark.table(TBL["dim_customer"]).filter("customer_sk <> -1").select(
    "customer_sk", F.col("customer_id").alias("c_id"), F.col("valid_from").alias("c_from"), F.col("valid_to").alias("c_to"))
prod = spark.table(TBL["dim_product"]).filter("product_sk <> -1").select(
    "product_sk", F.col("product_id").alias("p_id"), F.col("valid_from").alias("p_from"),
    F.col("valid_to").alias("p_to"), "unit_cost_inr")
store = spark.table(TBL["dim_store"]).select("store_sk", F.col("store_id").alias("s_id"))
fx = spark.table(TBL["silver_fx"]).select(F.col("rate_date").alias("fx_date"),
                                          F.col("currency").alias("fx_ccy"), "inr_per_unit")

fact_sales = (sales
    # point-in-time customer version
    .join(cust, (sales.customer_id == cust.c_id) & (sales.txn_ts >= cust.c_from) & (sales.txn_ts < cust.c_to), "left")
    # point-in-time product version (price/cost as they were on that day)
    .join(prod, (sales.product_id == prod.p_id) & (sales.txn_ts >= prod.p_from) & (sales.txn_ts < prod.p_to), "left")
    .join(store, sales.store_id == store.s_id, "left")
    .join(fx, (F.to_date(sales.txn_ts) == fx.fx_date) & (sales.currency == fx.fx_ccy), "left")
    .select("sales_line_id", "transaction_id",
            F.date_format("txn_ts", "yyyyMMdd").cast("int").alias("date_key"), "txn_ts",
            F.coalesce("customer_sk", F.lit(-1)).cast("long").alias("customer_sk"),
            F.coalesce("product_sk", F.lit(-1)).cast("long").alias("product_sk"),
            F.coalesce("store_sk", F.lit(-1)).cast("int").alias("store_sk"),
            "channel", "transaction_type", "payment_method", "currency",
            "quantity", "unit_price_local", "discount_pct", "gross_amount_local", "net_amount_local",
            F.col("inr_per_unit").alias("fx_inr_per_unit"),
            F.round(F.col("gross_amount_local") * F.col("inr_per_unit"), 2).alias("gross_amount_inr"),
            F.round(F.col("net_amount_local") * F.col("inr_per_unit"), 2).alias("net_revenue_inr"),
            F.round(F.when(F.col("transaction_type") == "sale", F.col("quantity"))
                     .when(F.col("transaction_type") == "return", -F.col("quantity")).otherwise(0)
                    * F.col("unit_cost_inr"), 2).alias("cogs_inr"))
    .withColumn("gross_margin_inr", F.round(F.col("net_revenue_inr") - F.col("cogs_inr"), 2)))

overwrite(fact_sales, TBL["fact_sales"])

missing_fx = spark.table(TBL["fact_sales"]).filter("fx_inr_per_unit IS NULL").count()
if missing_fx:
    print(f"WARNING: {missing_fx} sales rows have no FX rate (load more FX history).")

# %% [markdown]
# ## Other facts

# %%
cur_prod = spark.table(TBL["dim_product"]).filter("is_current").select("product_sk", F.col("product_id").alias("p_id"), "unit_cost_inr")
fact_inv = (spark.table(TBL["silver_inventory"]).alias("i")
            .join(cur_prod, F.col("i.product_id") == F.col("p_id"), "left")
            .join(store, F.col("i.store_id") == F.col("s_id"), "left")
            .select(F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
                    F.coalesce("store_sk", F.lit(-1)).alias("store_sk"),
                    F.coalesce("product_sk", F.lit(-1)).alias("product_sk"),
                    "on_hand_qty", "reorder_point",
                    (F.col("on_hand_qty") <= F.col("reorder_point")).alias("is_below_reorder"),
                    (F.col("on_hand_qty") == 0).alias("is_stockout"),
                    F.round(F.col("on_hand_qty") * F.col("unit_cost_inr"), 2).alias("stock_value_inr")))
overwrite(fact_inv, TBL["fact_inventory"])

cases = spark.table(TBL["silver_case"])
fact_case = (cases.join(cust, (cases.customer_id == cust.c_id) & (cases.opened_at >= cust.c_from) & (cases.opened_at < cust.c_to), "left")
             .select("case_id",
                     F.coalesce("customer_sk", F.lit(-1)).alias("customer_sk"),
                     F.date_format("opened_at", "yyyyMMdd").cast("int").alias("opened_date_key"),
                     F.date_format("resolved_at", "yyyyMMdd").cast("int").alias("resolved_date_key"),
                     "status", "is_open", "priority", "origin", "reason", "resolution_hours", "csat_score"))
overwrite(fact_case, TBL["fact_case"])

rev = spark.table(TBL["silver_review"])
fact_review = (rev.join(cust, (rev.customer_id == cust.c_id) & (rev.submitted_at >= cust.c_from) & (rev.submitted_at < cust.c_to), "left")
               .join(prod, (rev.product_id == prod.p_id) & (rev.submitted_at >= prod.p_from) & (rev.submitted_at < prod.p_to), "left")
               .select("review_id",
                       F.date_format("submitted_at", "yyyyMMdd").cast("int").alias("date_key"),
                       F.coalesce("customer_sk", F.lit(-1)).alias("customer_sk"),
                       F.coalesce("product_sk", F.lit(-1)).alias("product_sk"),
                       "rating", "source", "helpful_votes"))
overwrite(fact_review, TBL["fact_review"])

# %% [markdown]
# ## Table maintenance
# Small files hurt Direct Lake and SQL endpoint performance. V-Order + OPTIMIZE keep them fast.

# %%
for t in [TBL["fact_sales"], TBL["fact_inventory"], TBL["silver_pos"]]:
    if table_exists(spark, t):
        spark.sql(f"OPTIMIZE {t}")
print("Gold build complete")
