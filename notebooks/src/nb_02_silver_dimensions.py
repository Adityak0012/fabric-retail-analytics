# %% [markdown]
# # nb_02_silver_dimensions — mirrored SQL tables ➜ Silver (SCD Type 2)
#
# * `silver_customer` — **SCD2** on city/state/country/loyalty tier/email/opt-in
# * `silver_product`  — **SCD2** on price, cost, category, brand, active flag
# * `silver_store`    — SCD1 (overwrite changes, no history needed)
#
# The `src_*` tables are OneLake shortcuts to the **mirrored** SQL Server database, so
# they always hold the latest replicated state. Each run compares that state with the
# current Silver version and writes a new version for anything that changed.
#
# > Interview note: because we compare *snapshots*, two changes to the same customer
# > between runs collapse into one version. To capture every intermediate change you'd read
# > the CDC change feed instead. Know this trade-off — it's a common follow-up question.

# %% tags=["parameters"]
pipeline_run_id = "interactive"

# %%
# MAGIC %run nb_00_common

# %% [markdown]
# ## Customers (SCD2)

# %%
cust_src = (spark.table(TBL["src_customers"])
            .select(F.col("customer_id").cast("int"),
                    F.trim("first_name").alias("first_name"),
                    F.trim("last_name").alias("last_name"),
                    F.lower(F.trim("email")).alias("email"),
                    F.regexp_replace("phone", r"[^0-9]", "").alias("phone"),
                    F.initcap(F.trim("city")).alias("city"),
                    F.trim("state").alias("state"),
                    F.trim("country").alias("country"),
                    F.initcap("loyalty_tier").alias("loyalty_tier"),
                    F.col("marketing_opt_in").cast("boolean"),
                    F.col("signup_date").cast("date"),
                    F.col("updated_at").cast("timestamp").alias("source_updated_at")))

m = scd2_merge(spark, cust_src, TBL["silver_customer"],
               keys=["customer_id"],
               tracked=["email", "city", "state", "country", "loyalty_tier", "marketing_opt_in"],
               sk_col="customer_sk", effective_col="source_updated_at", handle_deletes=True)
log_run(spark, "nb_02_silver_dimensions", TBL["silver_customer"], m)

# %% [markdown]
# ## Products (SCD2)

# %%
prod_src = (spark.table(TBL["src_products"])
            .select("product_id", F.trim("product_name").alias("product_name"),
                    "category", "brand",
                    F.col("unit_price_inr").cast("decimal(12,2)"),
                    F.col("unit_cost_inr").cast("decimal(12,2)"),
                    F.col("is_active").cast("boolean"),
                    F.col("updated_at").cast("timestamp").alias("source_updated_at")))

m = scd2_merge(spark, prod_src, TBL["silver_product"],
               keys=["product_id"],
               tracked=["product_name", "category", "brand", "unit_price_inr", "unit_cost_inr", "is_active"],
               sk_col="product_sk", effective_col="source_updated_at")
log_run(spark, "nb_02_silver_dimensions", TBL["silver_product"], m)

# %% [markdown]
# ## Stores (SCD1)

# %%
store_src = (spark.table(TBL["src_stores"])
             .select("store_id", "store_name", "city", "state", "country", "currency",
                     "store_format", F.col("opened_date").cast("date")))
m = merge_upsert(spark, store_src, TBL["silver_store"], ["store_id"])
log_run(spark, "nb_02_silver_dimensions", TBL["silver_store"], m)

# %% [markdown]
# ## See the history you just built

# %%
display(spark.sql(f"""
    SELECT customer_id, city, loyalty_tier, email, valid_from, valid_to, is_current
    FROM {TBL['silver_customer']}
    WHERE customer_id IN (SELECT customer_id FROM {TBL['silver_customer']}
                          GROUP BY customer_id HAVING count(*) > 1 LIMIT 5)
    ORDER BY customer_id, valid_from"""))
