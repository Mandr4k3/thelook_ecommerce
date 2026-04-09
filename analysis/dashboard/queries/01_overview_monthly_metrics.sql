/*
Section 1: overview KPI inputs
Purpose:
- Rebuild monthly completed-order revenue, orders, units, and AOV
- Rebuild the monthly conversion proxy from Part 1

Run from repo root.
*/

WITH completed_items AS (
  SELECT
    id,
    order_id,
    user_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    sale_price
  FROM read_parquet('thelook_parquet_data/order_items.parquet')
  WHERE status = 'Complete'
    AND returned_at IS NULL
    AND CAST(created_at AS DATE) >= DATE '2020-01-01'
),
sales_metrics AS (
  SELECT
    CAST(date_trunc('month', created_at) AS DATE) AS month,
    SUM(sale_price) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(*) AS units,
    SUM(sale_price) / COUNT(DISTINCT order_id) AS aov
  FROM completed_items
  GROUP BY 1
),
event_metrics AS (
  SELECT
    CAST(date_trunc('month', CAST(created_at AS TIMESTAMP)) AS DATE) AS month,
    COUNT(DISTINCT CASE WHEN event_type = 'department' THEN user_id END) AS top_funnel_users,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS converted_users
  FROM read_parquet('thelook_parquet_data/events.parquet')
  WHERE CAST(created_at AS DATE) >= DATE '2020-01-01'
  GROUP BY 1
)
SELECT
  s.month,
  s.revenue,
  s.orders,
  s.units,
  s.aov,
  e.top_funnel_users,
  e.converted_users,
  e.converted_users * 1.0 / NULLIF(e.top_funnel_users, 0) AS monthly_conversion_rate_proxy
FROM sales_metrics s
LEFT JOIN event_metrics e USING (month)
ORDER BY s.month;
