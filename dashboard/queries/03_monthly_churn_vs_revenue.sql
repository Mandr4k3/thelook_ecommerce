/*
Section 3: monthly churn vs revenue
Definition:
- A user-month is churned if there is no completed order within 90 days
  after the last completed order in that month.
- Recent months are censored until a full 90-day observation window exists.
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
order_sequence AS (
  SELECT
    user_id,
    order_id,
    order_ts,
    LEAD(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts) AS next_order_ts
  FROM completed_orders
),
user_month_activity AS (
  SELECT
    user_id,
    CAST(date_trunc('month', order_ts) AS DATE) AS month,
    MAX(order_ts) AS last_order_ts_in_month
  FROM completed_orders
  GROUP BY 1, 2
),
data_boundary AS (
  SELECT MAX(order_ts) AS max_order_ts
  FROM completed_orders
),
eligible_user_months AS (
  SELECT
    uma.user_id,
    uma.month,
    uma.last_order_ts_in_month
  FROM user_month_activity uma
  CROSS JOIN data_boundary db
  WHERE uma.last_order_ts_in_month + INTERVAL 90 DAY <= db.max_order_ts
),
monthly_churn AS (
  SELECT
    eum.month,
    COUNT(DISTINCT eum.user_id) AS active_customers,
    COUNT(DISTINCT CASE
      WHEN os.next_order_ts IS NULL OR os.next_order_ts > eum.last_order_ts_in_month + INTERVAL 90 DAY
      THEN eum.user_id
    END) AS churned_customers_90d
  FROM eligible_user_months eum
  LEFT JOIN order_sequence os
    ON eum.user_id = os.user_id
   AND eum.last_order_ts_in_month = os.order_ts
  GROUP BY 1
),
monthly_revenue AS (
  SELECT
    CAST(date_trunc('month', order_ts) AS DATE) AS month,
    SUM(order_value) AS revenue
  FROM completed_orders
  GROUP BY 1
)
SELECT
  mc.month,
  mc.active_customers,
  mc.churned_customers_90d,
  mc.churned_customers_90d * 1.0 / NULLIF(mc.active_customers, 0) AS churn_rate_90d,
  mr.revenue
FROM monthly_churn mc
LEFT JOIN monthly_revenue mr USING (month)
ORDER BY mc.month;
