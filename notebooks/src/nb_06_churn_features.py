# %% [markdown]
# # nb_06_churn_features — feature engineering for the churn model
# Defines `build_features(as_of, with_label)` and the feature lists. It is not run on its
# own; nb_07 (train) and nb_08 (score) pull it in with `%run`, so training and scoring
# always use *identical* feature logic (no training/serving skew).
#
# **Definition.** A customer is *active* at date `T` if they bought something in the
# 365 days before `T`. An active customer has *churned* if they make **no purchase in
# the 90 days after `T`**. Features only use data up to `T`; the label only uses data
# after `T` — no leakage.

# %%
# MAGIC %run nb_00_common

# %%
import numpy as np
import pandas as pd

churn_window_days = globals().get("churn_window_days", 90)

CATEGORICAL = ["loyalty_tier", "country"]
NUMERIC = ["recency_days", "tenure_days", "orders_90d", "orders_365d", "revenue_365d_inr",
           "avg_order_value_inr", "return_rate_365d", "mobile_share_365d",
           "cases_180d", "open_cases", "high_priority_cases_180d", "avg_resolution_hours",
           "avg_csat", "reviews_365d", "avg_rating_365d", "days_since_last_case",
           "marketing_opt_in"]


def build_features(as_of: str, with_label: bool) -> pd.DataFrame:
    """One row per customer active in the 365 days before `as_of`."""
    s = spark.sql(f"""
    WITH cust AS (   -- attributes as they were on the snapshot date (SCD2 point-in-time)
        SELECT customer_sk, customer_id, loyalty_tier, country, signup_date,
               CAST(marketing_opt_in AS INT) AS marketing_opt_in
        FROM {TBL['dim_customer']}
        WHERE customer_sk <> -1 AND valid_from <= TIMESTAMP'{as_of}' AND valid_to > TIMESTAMP'{as_of}'
    ),
    sales AS (       -- map every fact row back to the durable customer_id
        SELECT d.customer_id, f.txn_ts, f.transaction_id, f.transaction_type, f.channel, f.net_revenue_inr
        FROM {TBL['fact_sales']} f JOIN {TBL['dim_customer']} d ON f.customer_sk = d.customer_sk
        WHERE f.customer_sk <> -1
    ),
    hist AS (
        SELECT customer_id,
               datediff(DATE'{as_of}', max(CASE WHEN transaction_type='sale' THEN txn_ts END)) AS recency_days,
               count(DISTINCT CASE WHEN transaction_type='sale' AND txn_ts > DATE'{as_of}' - INTERVAL 90 DAYS THEN transaction_id END) AS orders_90d,
               count(DISTINCT CASE WHEN transaction_type='sale' THEN transaction_id END) AS orders_365d,
               coalesce(sum(net_revenue_inr), 0) AS revenue_365d_inr,
               count(DISTINCT CASE WHEN transaction_type='return' THEN transaction_id END)
                 / greatest(count(DISTINCT transaction_id), 1) AS return_rate_365d,
               avg(CASE WHEN channel='mobile_app' THEN 1.0 ELSE 0.0 END) AS mobile_share_365d
        FROM sales
        WHERE txn_ts <= TIMESTAMP'{as_of}' AND txn_ts > TIMESTAMP'{as_of}' - INTERVAL 365 DAYS
        GROUP BY customer_id
        HAVING orders_365d > 0
    ),
    cases AS (
        SELECT d.customer_id,
               count(CASE WHEN o.date >= DATE'{as_of}' - INTERVAL 180 DAYS THEN 1 END) AS cases_180d,
               count(CASE WHEN c.resolved_date_key IS NULL OR r.date > DATE'{as_of}' THEN 1 END) AS open_cases,
               count(CASE WHEN c.priority='High' AND o.date >= DATE'{as_of}' - INTERVAL 180 DAYS THEN 1 END) AS high_priority_cases_180d,
               avg(CASE WHEN r.date <= DATE'{as_of}' THEN c.resolution_hours END) AS avg_resolution_hours,
               avg(CASE WHEN r.date <= DATE'{as_of}' THEN c.csat_score END) AS avg_csat,
               datediff(DATE'{as_of}', max(o.date)) AS days_since_last_case
        FROM {TBL['fact_case']} c
        JOIN {TBL['dim_customer']} d ON c.customer_sk = d.customer_sk
        JOIN {TBL['dim_date']} o ON c.opened_date_key = o.date_key
        LEFT JOIN {TBL['dim_date']} r ON c.resolved_date_key = r.date_key
        WHERE o.date <= DATE'{as_of}'
        GROUP BY d.customer_id
    ),
    reviews AS (
        SELECT d.customer_id, count(*) AS reviews_365d, avg(v.rating) AS avg_rating_365d
        FROM {TBL['fact_review']} v
        JOIN {TBL['dim_customer']} d ON v.customer_sk = d.customer_sk
        JOIN {TBL['dim_date']} dt ON v.date_key = dt.date_key
        WHERE dt.date <= DATE'{as_of}' AND dt.date > DATE'{as_of}' - INTERVAL 365 DAYS
        GROUP BY d.customer_id
    ),
    future AS (
        SELECT DISTINCT customer_id FROM sales
        WHERE transaction_type='sale' AND txn_ts > TIMESTAMP'{as_of}'
          AND txn_ts <= TIMESTAMP'{as_of}' + INTERVAL {churn_window_days} DAYS
    )
    SELECT h.customer_id, c.customer_sk, c.loyalty_tier, c.country, c.marketing_opt_in,
           datediff(DATE'{as_of}', c.signup_date) AS tenure_days,
           h.recency_days, h.orders_90d, h.orders_365d, h.revenue_365d_inr,
           h.revenue_365d_inr / h.orders_365d AS avg_order_value_inr,
           h.return_rate_365d, h.mobile_share_365d,
           coalesce(k.cases_180d, 0) AS cases_180d, coalesce(k.open_cases, 0) AS open_cases,
           coalesce(k.high_priority_cases_180d, 0) AS high_priority_cases_180d,
           k.avg_resolution_hours, k.avg_csat,
           coalesce(k.days_since_last_case, 9999) AS days_since_last_case,
           coalesce(r.reviews_365d, 0) AS reviews_365d, r.avg_rating_365d,
           CASE WHEN fu.customer_id IS NULL THEN 1 ELSE 0 END AS churned
    FROM hist h
    JOIN cust c ON h.customer_id = c.customer_id
    LEFT JOIN cases k ON h.customer_id = k.customer_id
    LEFT JOIN reviews r ON h.customer_id = r.customer_id
    LEFT JOIN future fu ON h.customer_id = fu.customer_id
    """)
    pdf = s.toPandas()
    for col in NUMERIC:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("float64")
    if not with_label:
        pdf = pdf.drop(columns=["churned"])
    pdf["as_of"] = as_of
    return pdf

