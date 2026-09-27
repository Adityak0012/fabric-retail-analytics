"""Local end-to-end smoke test: runs every notebook against a fake lakehouse on your laptop.

It stands in for Fabric pieces you don't have locally:
  * src_* tables  <- generated CSVs (what Mirroring would replicate)
  * Files/landing <- generated blob files + synthetic FX JSON (what Copy activities land)
  * bronze_pos_events <- events from the POS producer (what Eventstream would write)
  * notebookutils / display() stubs, local MLflow file store

Then it changes some customers/products and re-runs Silver to prove SCD2 history works.

    pip install pyspark==3.5.* delta-spark==3.2.* "great_expectations>=1.3,<2" mlflow scikit-learn pandas faker
    cd data_generator && python generate_data.py && cd ..
    python tests/local_e2e.py
"""
import json
import os
import pathlib
import random
import shutil
import sys
import types
from datetime import date, datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "data_generator"))
from build_notebooks import parse_cells  # noqa: E402

WORK = pathlib.Path(os.environ.get("E2E_WORKDIR", ROOT / ".e2e_lakehouse"))
GEN = ROOT / "data_generator" / "output"
shutil.rmtree(WORK, ignore_errors=True)
WORK.mkdir(parents=True)
os.chdir(WORK)                                   # "Files/..." paths resolve here, like in Fabric

from pyspark.sql import SparkSession, functions as F  # noqa: E402

USE_SHIM = os.environ.get("USE_DELTA_SHIM", "0") == "1"   # 1 = no Delta jars available (offline/locked-down)
builder = (SparkSession.builder.master("local[2]").appName("e2e")
           .config("spark.sql.warehouse.dir", str(WORK / "Tables"))
           .config("spark.sql.shuffle.partitions", "4")
           .config("spark.ui.enabled", "false"))
if USE_SHIM:
    spark = builder.getOrCreate()
    sys.path.insert(0, str(ROOT / "tests"))
    import delta_shim  # noqa: E402
    delta_shim.install(spark)
else:
    from delta import configure_spark_with_delta_pip  # noqa: E402
    spark = configure_spark_with_delta_pip(
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def rewrite_table(name, df):
    """UPDATE/DELETE stand-in that works with or without Delta."""
    df.localCheckpoint(eager=True).write.mode("overwrite").format("delta").saveAsTable(name)

# ---------------- Fabric stubs --------------------------------------------------
nbu = types.ModuleType("notebookutils")
nbu.mssparkutils = types.SimpleNamespace(fs=types.SimpleNamespace(mkdirs=lambda p: os.makedirs(p, exist_ok=True)))
sys.modules["notebookutils"] = nbu
import mlflow  # noqa: E402
mlflow.set_tracking_uri(f"sqlite:///{WORK}/mlflow.db")   # Fabric provides its own tracking server


def display(df, n=10):
    try:
        df.show(n, truncate=60)
    except AttributeError:
        print(df)


def run_notebook(name: str, ns: dict | None = None, params: dict | None = None) -> dict:
    ns = ns if ns is not None else {"spark": spark, "display": display, "__name__": "__nb__"}
    for cell in parse_cells((ROOT / "notebooks" / "src" / f"{name}.py").read_text()):
        if cell["type"] != "code":
            continue
        code = "\n".join(cell["lines"])
        if code.startswith("%run "):
            run_notebook(code.split()[-1], ns)
            continue
        exec(compile(code, name, "exec"), ns)
        if "parameters" in cell["tags"] and params:
            ns.update(params)
    return ns


def banner(t):
    print("\n" + "=" * 80 + f"\n{t}\n" + "=" * 80)


# ---------------- 1. fake Mirroring: src_* tables -------------------------------
banner("Seeding src_* tables (stand-in for mirrored SQL Server)")
for t in ["stores", "products", "customers", "orders", "order_items"]:
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(GEN / "sql" / f"{t}.csv"))
    for c in ("updated_at", "order_ts"):
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(c))
    df.write.format("delta").saveAsTable(f"src_{t}")
    print(f"src_{t}: {df.count():,}")

# ---------------- 2. fake Copy activities: landing files -----------------------
banner("Landing files (stand-in for pipeline Copy activities)")
for sub in ["inventory", "reviews", "crm"]:
    shutil.copytree(GEN / "blob" / sub, WORK / "Files" / "landing" / sub)
fx_dir = WORK / "Files" / "landing" / "fx"
fx_dir.mkdir(parents=True)
import config as C  # noqa: E402
rows, d = [], date(2025, 2, 1)
while d <= date.today():
    if d.weekday() < 5:                     # like the real API: no weekend rates
        for ccy in ["USD", "GBP", "EUR", "SGD"]:
            rows.append({"date": str(d), "base": "INR", "quote": ccy,
                         "rate": round(1 / C.INR_PER_UNIT[ccy] * random.uniform(0.97, 1.03), 6)})
    d += timedelta(days=1)
(fx_dir / "fx_rates_backfill.json").write_text(json.dumps(rows))

# ---------------- 3. fake Eventstream: bronze_pos_events -----------------------
from pos_stream_producer import make_basket, load_reference  # noqa: E402
os.chdir(ROOT / "data_generator"); stores, products = load_reference(); os.chdir(WORK)
events = [e for _ in range(400) for e in make_basket(stores, products, None, None)]
events += [e for _ in range(30) for e in make_basket(stores, products, "return_spike", "S003")]
spark.createDataFrame(events).write.format("delta").saveAsTable("bronze_pos_events")
print(f"bronze_pos_events: {len(events)} (incl. duplicates on purpose)")

# ---------------- 4. run the notebooks ------------------------------------------
P = {"pipeline_run_id": "e2e-run-1"}
for nb in ["nb_01_bronze_ingest", "nb_02_silver_dimensions", "nb_03_silver_facts",
           "nb_04_data_quality", "nb_05_gold_star_schema"]:
    banner(nb)
    run_notebook(nb, params=P)

banner("Idempotency: re-run bronze + silver facts, counts must not change")
before = {t: spark.table(t).count() for t in ["bronze_inventory", "silver_order_line", "silver_pos_event", "silver_review"]}
run_notebook("nb_01_bronze_ingest", params=P)
run_notebook("nb_03_silver_facts", params=P)
after = {t: spark.table(t).count() for t in before}
print(before, after)
assert before == after, "re-run changed row counts — not idempotent!"

# ---------------- 5. simulate OLTP changes, prove SCD2 -----------------------------
banner("Simulating customer/product changes -> SCD2")
changed_ts = datetime.now().replace(microsecond=0)
hit = F.col("customer_id").isin(1, 2, 3)
c = spark.table("src_customers")
c = (c.withColumn("city", F.when(hit, "Mumbai").otherwise(F.col("city")))
      .withColumn("loyalty_tier", F.when(hit, "Gold").otherwise(F.col("loyalty_tier")))
      .withColumn("updated_at", F.when(hit, F.lit(changed_ts)).otherwise(F.col("updated_at")))
      .filter("customer_id <> 4999"))                                  # hard delete in source
rewrite_table("src_customers", c)
p = spark.table("src_products")
hitp = F.col("product_id") == "P0001"
p = (p.withColumn("unit_price_inr", F.when(hitp, F.col("unit_price_inr") * 1.2).otherwise(F.col("unit_price_inr")))
      .withColumn("updated_at", F.when(hitp, F.lit(changed_ts)).otherwise(F.col("updated_at"))))
rewrite_table("src_products", p)
run_notebook("nb_02_silver_dimensions", params={"pipeline_run_id": "e2e-run-2"})
hist = spark.sql("SELECT customer_id, city, loyalty_tier, valid_from, valid_to, is_current, is_deleted "
                 "FROM silver_customer WHERE customer_id IN (1,2,3,4999) ORDER BY customer_id, valid_from")
hist.show(truncate=False)
cnt = {r.customer_id: r.n for r in spark.sql(
    "SELECT customer_id, count(*) n FROM silver_customer WHERE customer_id IN (1,2,3) GROUP BY 1").collect()}
assert all(cnt[i] == 2 for i in (1, 2, 3)), f"expected 2 versions each, got {cnt}"
assert spark.sql("SELECT count(*) FROM silver_customer WHERE is_current GROUP BY customer_id HAVING count(*)>1").count() == 0
assert spark.sql("SELECT * FROM silver_customer WHERE customer_id=4999 AND is_deleted AND NOT is_current").count() == 1
# no-change re-run must not create versions
n_before = spark.table("silver_customer").count()
run_notebook("nb_02_silver_dimensions", params={"pipeline_run_id": "e2e-run-3"})
assert spark.table("silver_customer").count() == n_before, "no-op run created new versions!"
print("SCD2 checks passed")

run_notebook("nb_05_gold_star_schema", params={"pipeline_run_id": "e2e-run-2"})
banner("Point-in-time check: customer 1's old sales keep the old city")
spark.sql("""SELECT d.city, d.is_current, count(*) n FROM gold_fact_sales f
             JOIN gold_dim_customer d ON f.customer_sk = d.customer_sk
             WHERE d.customer_id = 1 GROUP BY 1,2""").show()
orphan = spark.sql("SELECT count(*) c FROM gold_fact_sales WHERE customer_sk = -1 AND channel <> 'in_store'").collect()[0].c
assert orphan == 0, f"{orphan} online sales lost their customer"
assert spark.sql("SELECT count(*) FROM gold_fact_sales WHERE fx_inr_per_unit IS NULL").collect()[0][0] == 0

# ---------------- 6. ML -------------------------------------------------------------
for nb in ["nb_07_churn_train", "nb_08_churn_score"]:
    banner(nb)
    run_notebook(nb, params=P)

banner("Quality gate must BLOCK bad data")
good_pos = spark.table("silver_pos_event").localCheckpoint(eager=True)
rewrite_table("silver_pos_event", good_pos.unionByName(good_pos.limit(5)))   # duplicate event_ids
try:
    run_notebook("nb_04_data_quality", params={"pipeline_run_id": "e2e-bad"})
    raise AssertionError("quality gate did not fail on duplicate event_ids")
except Exception as e:
    assert "Critical data-quality checks failed" in str(e), e
    print("gate correctly failed:", str(e).splitlines()[1].strip())
rewrite_table("silver_pos_event", good_pos)

banner("Ops tables")
spark.sql("SELECT severity, success, count(*) FROM ops_dq_results GROUP BY 1,2").show()
spark.sql("SELECT source_table, reason, count(*) FROM silver_quarantine GROUP BY 1,2").show(truncate=False)
spark.sql("SELECT risk_band, count(*), round(avg(churn_probability),3) FROM gold_churn_scores GROUP BY 1").show()
print("\nALL LOCAL E2E CHECKS PASSED")
