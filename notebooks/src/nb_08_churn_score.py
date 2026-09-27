# %% [markdown]
# # nb_08_churn_score — batch scoring into Gold
# Loads the latest registered `retail_churn_model`, scores every currently-active
# customer as of today, and writes `gold_churn_scores` for Power BI (risk bands, top
# at-risk customers) and for the REST/GraphQL API.

# %% tags=["parameters"]
pipeline_run_id = "interactive"
registered_model_name = "retail_churn_model"
churn_window_days = 90

# %%
# MAGIC %run nb_06_churn_features

# %%
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

client = MlflowClient()
versions = client.search_model_versions(f"name = '{registered_model_name}'")
latest = max(versions, key=lambda v: int(v.version))
model = mlflow.sklearn.load_model(f"models:/{registered_model_name}/{latest.version}")
print(f"Using {registered_model_name} v{latest.version} (run {latest.run_id})")

# %%
as_of = spark.sql(f"SELECT CAST(max(txn_ts) AS DATE) FROM {TBL['fact_sales']}").collect()[0][0].strftime("%Y-%m-%d")
feats = build_features(as_of, with_label=False)
feats["churn_probability"] = model.predict_proba(feats[NUMERIC + CATEGORICAL])[:, 1]
feats["risk_band"] = pd.cut(feats["churn_probability"], bins=[-0.01, 0.3, 0.6, 1.0],
                            labels=["Low", "Medium", "High"]).astype(str)
feats["model_version"] = str(latest.version)

scores = (spark.createDataFrame(feats[["customer_id", "customer_sk", "as_of", "churn_probability", "risk_band",
                                       "model_version", "recency_days", "orders_365d", "revenue_365d_inr",
                                       "cases_180d", "avg_rating_365d"]])
          .withColumn("as_of", F.to_date("as_of"))
          .withColumn("scored_at", F.current_timestamp()))
# keep history of scores (one snapshot per as_of date) — lets you track drift over time
if table_exists(spark, TBL["churn_scores"]):
    (scores.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"as_of = DATE'{as_of}'")   # re-running a day replaces only that day
           .saveAsTable(TBL["churn_scores"]))
else:
    scores.write.format("delta").saveAsTable(TBL["churn_scores"])

log_run(spark, "nb_08_churn_score", TBL["churn_scores"],
        {"as_of": as_of, "scored": len(feats), "model_version": latest.version,
         "high_risk": int((feats.risk_band == "High").sum())})
display(scores.orderBy(F.desc("churn_probability")).limit(20))
