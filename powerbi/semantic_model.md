# Power BI — Direct Lake semantic model & report

## 1. Create the model
`lh_retail` → **New semantic model** → name `Retail Analytics` → select:

`gold_dim_date, gold_dim_customer, gold_dim_product, gold_dim_store, gold_fact_sales,
gold_fact_inventory_daily, gold_fact_support_case, gold_fact_review, gold_churn_scores,
ops_dq_results`

This is a **Direct Lake** model: Power BI reads the Delta/Parquet files in OneLake directly — no
import copy, no scheduled refresh, yet import-like speed. (Interview line: "Direct Lake gives
DirectQuery freshness with Import performance, because V-Ordered Parquet is loaded straight into
VertiPaq memory.")

## 2. Relationships (Model view) — all single direction, many-to-one

| From (many) | To (one) |
|---|---|
| gold_fact_sales[date_key] | gold_dim_date[date_key] |
| gold_fact_sales[customer_sk] | gold_dim_customer[customer_sk] |
| gold_fact_sales[product_sk] | gold_dim_product[product_sk] |
| gold_fact_sales[store_sk] | gold_dim_store[store_sk] |
| gold_fact_inventory_daily[date_key] | gold_dim_date[date_key] |
| gold_fact_inventory_daily[product_sk] | gold_dim_product[product_sk] |
| gold_fact_inventory_daily[store_sk] | gold_dim_store[store_sk] |
| gold_fact_support_case[opened_date_key] | gold_dim_date[date_key] |
| gold_fact_support_case[customer_sk] | gold_dim_customer[customer_sk] |
| gold_fact_review[date_key] | gold_dim_date[date_key] |
| gold_fact_review[product_sk] | gold_dim_product[product_sk] |
| gold_fact_review[customer_sk] | gold_dim_customer[customer_sk] |
| gold_churn_scores[customer_sk] | gold_dim_customer[customer_sk] |

Then:
* Mark `gold_dim_date` as the **date table** (column `date`).
* Sort `month_name` by `month`, `weekday_name` by `weekday_num`.
* Hide all `*_sk` and `*_key` columns and technical columns (`row_hash`, `_bk`, `_updated_at`).
* Add the measures from [`measures.dax`](measures.dax) into a `_Measures` table.

> **SCD2 + slicing tip.** Because customers have several versions, slicing sales by
> `gold_dim_customer[city]` shows *where the customer lived when they bought* (historically correct).
> For "current city" reporting, add a calculated column or use `vw_customer_360`.

## 3. Report pages

| Page | Visuals |
|---|---|
| **Executive overview** | KPI cards: Net Revenue, YoY %, Gross Margin %, Orders, AOV · line: Net Revenue vs LY by month · bar: revenue by country · donut: channel mix · table: top 10 products |
| **Stores & real-time** | map/bar by store · 7-day rolling revenue · Return Rate % by store (the Activator demo store should stand out) |
| **Customers & churn** | High Risk Customers, Revenue at Risk · histogram of churn probability · risk band by loyalty tier · table of top at-risk customers with recency, cases, rating |
| **Inventory** | Stock Value, SKUs Below Reorder, Stockout Rate % · matrix store × category · table from latest snapshot |
| **Customer service** | Cases Opened / Open Cases trend · Avg Resolution Hours by reason · Avg CSAT vs Avg Review Rating |
| **Data quality monitor** | DQ Pass Rate %, Critical Failures · table of failed checks per run · quarantine count by reason (add `silver_quarantine` to the model if you want this) |

Design: one accent colour, consistent number formats (₹ with K/L/Cr or K/M), titles that state the
insight ("Returns spiked at S003 on 12 Oct") rather than the chart type.

## 4. Real-time tile (optional)
Eventstream → add an **Eventhouse** destination, then build a **Real-Time Dashboard** tile with KQL:
```kusto
pos_events
| where event_ts > ago(30m)
| summarize revenue = sum(line_amount) by bin(event_ts, 1m), store_id
| render timechart
```
Only do this after the core project works — it adds capacity usage.

## About "Copilot insights" from the original diagram
Copilot and Fabric data agents are **not available on the Fabric trial** capacity (they need a paid
F2+ capacity). Don't claim them on your resume unless you actually build them. If you later get
paid capacity, the Gold star schema with clear names and descriptions is exactly what Copilot needs.
