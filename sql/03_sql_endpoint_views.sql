/* =====================================================================
   T-SQL views on the lakehouse SQL analytics endpoint (lh_retail).
   Open lh_retail > switch to "SQL analytics endpoint" > New SQL query.
   These power the REST/GraphQL API and ad-hoc analysis. (Power BI uses
   the Direct Lake semantic model on the Delta tables directly.)
   ===================================================================== */

-- 1. Daily sales by store & channel, in INR ---------------------------
CREATE OR ALTER VIEW dbo.vw_sales_daily AS
SELECT  d.[date]                              AS sales_date,
        s.store_id, s.store_name, s.country,
        f.channel,
        COUNT(DISTINCT f.transaction_id)      AS transactions,
        SUM(CASE WHEN f.transaction_type = 'sale'   THEN f.quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN f.transaction_type = 'return' THEN f.quantity ELSE 0 END) AS units_returned,
        SUM(f.net_revenue_inr)                AS net_revenue_inr,
        SUM(f.gross_margin_inr)               AS gross_margin_inr
FROM    dbo.gold_fact_sales f
JOIN    dbo.gold_dim_date  d ON d.date_key = f.date_key
JOIN    dbo.gold_dim_store s ON s.store_sk = f.store_sk
GROUP BY d.[date], s.store_id, s.store_name, s.country, f.channel;
GO

-- 2. Customer 360 (current attributes + latest churn score) -----------
CREATE OR ALTER VIEW dbo.vw_customer_360 AS
WITH latest_score AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY as_of DESC) AS rn
    FROM dbo.gold_churn_scores
),
sales AS (
    SELECT dc.customer_id,
           SUM(f.net_revenue_inr)          AS lifetime_revenue_inr,
           COUNT(DISTINCT f.transaction_id) AS lifetime_orders,
           MAX(f.txn_ts)                   AS last_purchase_ts
    FROM dbo.gold_fact_sales f
    JOIN dbo.gold_dim_customer dc ON dc.customer_sk = f.customer_sk
    WHERE f.transaction_type = 'sale'
    GROUP BY dc.customer_id
)
SELECT  c.customer_id, c.customer_name, c.email, c.city, c.country, c.loyalty_tier,
        c.signup_date,
        s.lifetime_revenue_inr, s.lifetime_orders, s.last_purchase_ts,
        ls.churn_probability, ls.risk_band, ls.as_of AS scored_as_of
FROM    dbo.gold_dim_customer c
LEFT JOIN sales s         ON s.customer_id = c.customer_id
LEFT JOIN latest_score ls ON ls.customer_id = c.customer_id AND ls.rn = 1
WHERE   c.is_current = 1 AND c.customer_sk <> -1;
GO

-- 3. Inventory needing action (latest snapshot) ------------------------
CREATE OR ALTER VIEW dbo.vw_inventory_alerts AS
SELECT  d.[date] AS snapshot_date, s.store_id, s.store_name,
        p.product_id, p.product_name, p.category,
        i.on_hand_qty, i.reorder_point, i.is_stockout, i.stock_value_inr
FROM    dbo.gold_fact_inventory_daily i
JOIN    dbo.gold_dim_date d    ON d.date_key = i.date_key
JOIN    dbo.gold_dim_store s   ON s.store_sk = i.store_sk
JOIN    dbo.gold_dim_product p ON p.product_sk = i.product_sk
WHERE   i.date_key = (SELECT MAX(date_key) FROM dbo.gold_fact_inventory_daily)
  AND   i.is_below_reorder = 1;
GO

-- 4. Data-quality scorecard per run --------------------------------------
CREATE OR ALTER VIEW dbo.vw_dq_scorecard AS
SELECT  run_id, table_name,
        MIN(validated_at)                                   AS validated_at,
        COUNT(*)                                            AS checks,
        SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END)        AS passed,
        SUM(CASE WHEN success = 0 AND severity = 'critical' THEN 1 ELSE 0 END) AS critical_failed,
        SUM(CASE WHEN success = 0 AND severity = 'warning'  THEN 1 ELSE 0 END) AS warnings,
        CAST(100.0 * SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,1)) AS pass_pct
FROM    dbo.ops_dq_results
GROUP BY run_id, table_name;
GO

-- Handy checks -------------------------------------------------------------
-- SCD2 sanity: exactly one current row per customer (expect 0 rows)
SELECT customer_id, COUNT(*) AS current_versions
FROM dbo.silver_customer WHERE is_current = 1
GROUP BY customer_id HAVING COUNT(*) > 1;

-- Customers with the most history
SELECT TOP 10 customer_id, COUNT(*) AS versions
FROM dbo.silver_customer GROUP BY customer_id ORDER BY versions DESC;
