# %% [markdown]
# # nb_04_data_quality — Great Expectations quality gate on Silver
#
# * Runs a GX (Great Expectations 1.x) suite against each Silver table.
# * Every expectation carries `meta.severity`: **critical** failures stop the pipeline
#   *before* Gold is rebuilt (bad data never reaches the dashboards); **warning**
#   failures are recorded but don't block.
# * All results land in `ops_dq_results` → the "Data quality monitor" Power BI page.
#
# **Setup:** create a Fabric *Environment* item, add the public library
# `great_expectations` (pin a 1.x version), publish it, and select that environment on
# this notebook. (Environments are more reliable in scheduled runs than `%pip install`.)

# %% tags=["parameters"]
pipeline_run_id = "interactive"
fail_on_critical = True

# %%
# MAGIC %run nb_00_common

# %%
import great_expectations as gx
from great_expectations import expectations as gxe

CRIT, WARN = {"severity": "critical"}, {"severity": "warning"}

SUITES = {
    TBL["silver_customer"]: {
        "filter": "is_current",
        "expectations": [
            gxe.ExpectColumnValuesToNotBeNull(column="customer_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeUnique(column="customer_id", meta=CRIT),   # one current row per key
            gxe.ExpectColumnValuesToMatchRegex(column="email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                                               mostly=0.99, meta=WARN),
            gxe.ExpectColumnValuesToBeInSet(column="loyalty_tier",
                                            value_set=["Bronze", "Silver", "Gold", "Platinum"], meta=CRIT),
        ],
    },
    TBL["silver_product"]: {
        "filter": "is_current",
        "expectations": [
            gxe.ExpectColumnValuesToBeUnique(column="product_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeBetween(column="unit_price_inr", min_value=1, max_value=500000, meta=CRIT),
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="unit_price_inr", column_B="unit_cost_inr",
                                                        or_equal=True, mostly=0.98, meta=WARN),
        ],
    },
    TBL["silver_order_line"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "line_no"], meta=CRIT),
            gxe.ExpectColumnValuesToNotBeNull(column="customer_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=100, meta=CRIT),
            gxe.ExpectColumnValuesToBeInSet(column="order_status",
                                            value_set=["completed", "cancelled", "returned"], meta=CRIT),
            gxe.ExpectColumnValuesToBeInSet(column="currency",
                                            value_set=["INR", "USD", "GBP", "EUR", "SGD"], meta=CRIT),
        ],
    },
    TBL["silver_pos"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectColumnValuesToBeUnique(column="event_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=["sale", "return"], meta=CRIT),
            gxe.ExpectColumnValuesToNotBeNull(column="store_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=50, mostly=0.995, meta=WARN),
        ],
    },
    TBL["silver_inventory"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectColumnValuesToBeBetween(column="on_hand_qty", min_value=0, meta=CRIT),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["snapshot_date", "store_id", "product_id"], meta=CRIT),
        ],
    },
    TBL["silver_review"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectColumnValuesToBeUnique(column="review_id", meta=CRIT),
            gxe.ExpectColumnValuesToBeBetween(column="rating", min_value=1, max_value=5, meta=CRIT),
            gxe.ExpectColumnValuesToNotBeNull(column="product_id", meta=CRIT),
        ],
    },
    TBL["silver_case"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectColumnValuesToBeUnique(column="case_id", meta=CRIT),
            gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.99, meta=WARN),
            gxe.ExpectColumnValuesToBeInSet(column="priority", value_set=["Low", "Medium", "High"], meta=WARN),
        ],
    },
    TBL["silver_fx"]: {
        "filter": None,
        "expectations": [
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["rate_date", "currency"], meta=CRIT),
            gxe.ExpectColumnValuesToNotBeNull(column="inr_per_unit", meta=CRIT),
        ],
    },
}

# %%
context = gx.get_context(mode="ephemeral")
datasource = context.data_sources.add_spark(name="lakehouse")
rows, critical_failures = [], []

for table, cfg in SUITES.items():
    if not table_exists(spark, table):
        print(f"skip {table} (not built yet)")
        continue
    df = spark.table(table)
    if cfg["filter"]:
        df = df.filter(cfg["filter"])
    asset = datasource.add_dataframe_asset(name=table)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{table}_all")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
    for e in cfg["expectations"]:
        suite.add_expectation(e)
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{table}_validation", data=batch_def, suite=suite))
    result = vd.run(batch_parameters={"dataframe": df})

    for r in result.results:
        ec = r.expectation_config
        sev = (ec.meta or {}).get("severity", "warning")
        res = r.result or {}
        cols = ec.kwargs.get("column") or ",".join(ec.kwargs.get("column_list", []) or
                                                    [ec.kwargs.get("column_A", ""), ec.kwargs.get("column_B", "")]).strip(",")
        rows.append((pipeline_run_id, table, ec.type, cols, sev, bool(r.success),
                     res.get("element_count"), res.get("unexpected_count"),
                     float(res["unexpected_percent"]) if res.get("unexpected_percent") is not None else None,
                     str(res.get("observed_value")) if res.get("observed_value") is not None else None))
        if not r.success and sev == "critical":
            critical_failures.append(f"{table}: {ec.type}({cols})")

# %%
schema = ("run_id string, table_name string, expectation string, columns string, severity string, "
          "success boolean, element_count long, unexpected_count long, unexpected_percent double, "
          "observed_value string")
dq = spark.createDataFrame(rows, schema).withColumn("validated_at", F.current_timestamp())
dq.write.format("delta").mode("append").saveAsTable(TBL["dq_results"])
display(dq.orderBy("success", "severity", "table_name"))

summary = {"checks": len(rows), "failed": sum(1 for r in rows if not r[5]),
           "critical_failed": len(critical_failures)}
log_run(spark, "nb_04_data_quality", TBL["dq_results"], summary,
        "failed" if critical_failures else "succeeded")

# %%
if critical_failures and fail_on_critical:
    # raising makes the notebook activity fail -> pipeline stops before Gold and sends the alert
    raise Exception("Critical data-quality checks failed:\n  " + "\n  ".join(critical_failures))
print("Quality gate passed:", summary)
