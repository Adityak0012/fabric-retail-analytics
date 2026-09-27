# %% [markdown]
# # nb_07_churn_train — churn model with MLflow tracking
#
# **Out-of-time validation.** Train on snapshot `T1 = last date − 180d`, evaluate on
# `T2 = last date − 90d` (a later period the model never saw). That mimics production far
# better than a random split. The registered model is then refit on both snapshots.
#
# Every run is tracked in the Fabric **ML experiment** `retail-churn-experiment`
# (params, metrics, feature importance, model with signature) and the final model is
# registered as `retail_churn_model`. Feature logic lives in `nb_06_churn_features`.

# %% tags=["parameters"]
pipeline_run_id = "interactive"
experiment_name = "retail-churn-experiment"
registered_model_name = "retail_churn_model"
churn_window_days = 90

# %%
# MAGIC %run nb_06_churn_features

# %%
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# %%
last_date = spark.sql(f"SELECT max(txn_ts) FROM {TBL['fact_sales']} WHERE channel <> 'in_store'").collect()[0][0]
T1 = (last_date - pd.Timedelta(days=2 * churn_window_days)).strftime("%Y-%m-%d")
T2 = (last_date - pd.Timedelta(days=churn_window_days)).strftime("%Y-%m-%d")
train, valid = build_features(T1, True), build_features(T2, True)
print(f"train as_of={T1}: {len(train):,} customers, churn rate {train.churned.mean():.1%}")
print(f"valid as_of={T2}: {len(valid):,} customers, churn rate {valid.churned.mean():.1%}")

# %%
def make_pipeline(kind: str) -> Pipeline:
    if kind == "logreg":
        num = Pipeline([("impute", _imputer()), ("scale", StandardScaler())])
        clf = LogisticRegression(max_iter=2000, class_weight="balanced")
    else:
        num = _imputer()
        clf = HistGradientBoostingClassifier(learning_rate=0.06, max_iter=300, max_leaf_nodes=31,
                                             l2_regularization=1.0, class_weight="balanced",
                                             early_stopping=True, random_state=42)
    pre = ColumnTransformer([("num", num, NUMERIC),
                             ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL)])
    return Pipeline([("prep", pre), ("model", clf)])


def _imputer():
    return SimpleImputer(strategy="median", add_indicator=True)


def precision_at_top(y, p, frac=0.1):
    k = max(int(len(p) * frac), 1)
    idx = np.argsort(-p)[:k]
    return float(np.asarray(y)[idx].mean())


def evaluate(model, X, y) -> dict:
    p = model.predict_proba(X)[:, 1]
    return {"roc_auc": roc_auc_score(y, p), "pr_auc": average_precision_score(y, p),
            "brier": brier_score_loss(y, p), "precision_top10pct": precision_at_top(y, p),
            "base_rate": float(np.mean(y))}

# %%
mlflow.set_experiment(experiment_name)
X_cols = NUMERIC + CATEGORICAL
results = {}
for kind in ["logreg", "hist_gbm"]:
    with mlflow.start_run(run_name=f"{kind}-oot-{T2}") as run:
        model = make_pipeline(kind).fit(train[X_cols], train.churned)
        metrics = evaluate(model, valid[X_cols], valid.churned)
        mlflow.log_params({"model_type": kind, "train_as_of": T1, "valid_as_of": T2,
                           "churn_window_days": churn_window_days, "n_train": len(train),
                           "n_valid": len(valid), "features": ",".join(X_cols)})
        mlflow.log_metrics({f"valid_{k}": v for k, v in metrics.items()})
        results[kind] = (metrics, run.info.run_id)
        print(kind, {k: round(v, 3) for k, v in metrics.items()})

best_kind = max(results, key=lambda k: results[k][0]["pr_auc"])
print("best model:", best_kind)

# %% [markdown]
# ## Refit the winner on both snapshots, explain it, register it

# %%
full = pd.concat([train, valid], ignore_index=True)
with mlflow.start_run(run_name=f"{best_kind}-final-{T2}") as run:
    final = make_pipeline(best_kind).fit(full[X_cols], full.churned)
    mlflow.log_params({"model_type": best_kind, "trained_on": f"{T1},{T2}", "n_rows": len(full),
                       "selected_by": "valid_pr_auc"})
    mlflow.log_metrics({f"oot_{k}": v for k, v in results[best_kind][0].items()})

    imp = permutation_importance(final, valid[X_cols], valid.churned, scoring="average_precision",
                                 n_repeats=5, random_state=42)
    fi = (pd.DataFrame({"feature": X_cols, "importance": imp.importances_mean})
            .sort_values("importance", ascending=False))
    mlflow.log_table(fi, "feature_importance.json")
    print(fi.head(10).to_string(index=False))

    sig = infer_signature(valid[X_cols], final.predict_proba(valid[X_cols])[:, 1])
    mlflow.sklearn.log_model(final, artifact_path="model", signature=sig,
                             input_example=valid[X_cols].head(3),
                             serialization_format="cloudpickle",   # same format on MLflow 2.x (Fabric) and 3.x
                             registered_model_name=registered_model_name)
    log_run(spark, "nb_07_churn_train", registered_model_name,
            {"best": best_kind, **{k: round(v, 4) for k, v in results[best_kind][0].items()},
             "mlflow_run_id": run.info.run_id})
