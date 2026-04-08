/*
Section 2: new vs returning revenue mix by month
Definitions:
- Active customer: >= 1 completed, non-returned order in the month
- New customer: first-ever completed order month
- Returning customer: any later active month
*/

WITH completed_orders AS (
  SELECT
    user_id,
    order_id,
    MIN(CAST(created_at AS TIMESTAMP)) AS order_ts,
    SUM(sale_price) AS order_value
  FROM read_parquet('thelook_parquet_data/order_items.parquet')
  WHERE status = 'Complete'
    AND returned_at IS NULL
    AND CAST(created_at AS DATE) >= DATE '2020-01-01'
  GROUP BY 1, 2
),
first_purchase AS (
  SELECT
    user_id,
    CAST(date_trunc('month', MIN(order_ts)) AS DATE) AS first_purchase_month
  FROM completed_orders
  GROUP BY 1
),
monthly_user_sales AS (
  SELECT
    user_id,
    CAST(date_trunc('month', order_ts) AS DATE) AS month,
    SUM(order_value) AS user_revenue
  FROM completed_orders
  GROUP BY 1, 2
)
SELECT
  mus.month,
  COUNT(DISTINCT mus.user_id) AS active_customers,
  COUNT(DISTINCT CASE WHEN mus.month = fp.first_purchase_month THEN mus.user_id END) AS new_customers,
  COUNT(DISTINCT CASE WHEN mus.month > fp.first_purchase_month THEN mus.user_id END) AS returning_customers,
  SUM(CASE WHEN mus.month = fp.first_purchase_month THEN mus.user_revenue ELSE 0 END) AS new_revenue,
  SUM(CASE WHEN mus.month > fp.first_purchase_month THEN mus.user_revenue ELSE 0 END) AS returning_revenue,
  SUM(CASE WHEN mus.month > fp.first_purchase_month THEN mus.user_revenue ELSE 0 END)
    / NULLIF(SUM(mus.user_revenue), 0) AS returning_share
FROM monthly_user_sales mus
JOIN first_purchase fp USING (user_id)
GROUP BY 1
ORDER BY 1;
