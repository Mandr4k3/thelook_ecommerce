/*
Section 4: customer segment split
Snapshot date:
- last day of the last full month in the dataset

Segments:
- Champions
- Active repeat
- Newly acquired
- At risk
- Dormant
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
  GROUP BY 1, 2
),
snapshot_context AS (
  SELECT
    CAST(date_trunc('month', MAX(order_ts)) AS DATE) - INTERVAL 1 DAY AS snapshot_date
  FROM completed_orders
),
customer_snapshot AS (
  SELECT
    co.user_id,
    MIN(CAST(co.order_ts AS DATE)) AS first_purchase_date,
    MAX(CAST(co.order_ts AS DATE)) AS last_purchase_date,
    COUNT(DISTINCT co.order_id) AS lifetime_orders,
    SUM(co.order_value) AS lifetime_revenue
  FROM completed_orders co
  CROSS JOIN snapshot_context sc
  WHERE CAST(co.order_ts AS DATE) <= sc.snapshot_date
  GROUP BY 1
),
revenue_threshold AS (
  SELECT
    quantile_cont(lifetime_revenue, 0.75) AS high_value_threshold
  FROM customer_snapshot
),
segmented AS (
  SELECT
    cs.user_id,
    cs.lifetime_orders,
    cs.lifetime_revenue,
    date_diff('day', cs.last_purchase_date, sc.snapshot_date) AS recency_days,
    CASE
      WHEN date_diff('day', cs.last_purchase_date, sc.snapshot_date) <= 90
           AND cs.lifetime_orders >= 3
           AND cs.lifetime_revenue >= rt.high_value_threshold THEN 'Champions'
      WHEN date_diff('day', cs.last_purchase_date, sc.snapshot_date) <= 90
           AND cs.lifetime_orders >= 2 THEN 'Active repeat'
      WHEN date_diff('day', cs.last_purchase_date, sc.snapshot_date) <= 90 THEN 'Newly acquired'
      WHEN date_diff('day', cs.last_purchase_date, sc.snapshot_date) <= 180 THEN 'At risk'
      ELSE 'Dormant'
    END AS segment
  FROM customer_snapshot cs
  CROSS JOIN snapshot_context sc
  CROSS JOIN revenue_threshold rt
)
SELECT
  segment,
  COUNT(DISTINCT user_id) AS customers,
  SUM(lifetime_revenue) AS revenue,
  AVG(lifetime_orders) AS avg_orders,
  COUNT(DISTINCT user_id) * 1.0 / SUM(COUNT(DISTINCT user_id)) OVER () AS customer_share,
  SUM(lifetime_revenue) * 1.0 / SUM(SUM(lifetime_revenue)) OVER () AS revenue_share
FROM segmented
GROUP BY 1
ORDER BY CASE segment
  WHEN 'Dormant' THEN 1
  WHEN 'At risk' THEN 2
  WHEN 'Newly acquired' THEN 3
  WHEN 'Active repeat' THEN 4
  WHEN 'Champions' THEN 5
END;
