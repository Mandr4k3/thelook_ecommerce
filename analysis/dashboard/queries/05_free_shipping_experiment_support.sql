/*
Section 5: free shipping experiment support
This section uses three supporting queries:
1. pre/post proxy around 2022-01-15
2. last-12-month order value bands
3. first-order repeat by acquisition channel
*/

-- 1) Pre / post proxy baseline
WITH completed_orders AS (
  SELECT
    user_id,
    order_id,
    MIN(CAST(created_at AS TIMESTAMP)) AS order_ts,
    SUM(sale_price) AS order_value
  FROM read_parquet('thelook_parquet_data/order_items.parquet')
  WHERE status = 'Complete'
    AND returned_at IS NULL
    AND CAST(created_at AS DATE) BETWEEN DATE '2021-12-16' AND DATE '2022-02-14'
  GROUP BY 1, 2
)
SELECT
  CASE
    WHEN CAST(order_ts AS DATE) < DATE '2022-01-15' THEN 'Pre-launch'
    ELSE 'Post-launch'
  END AS period,
  COUNT(DISTINCT order_id) AS completed_orders,
  AVG(order_value) AS avg_order_value,
  median(order_value) AS median_order_value,
  AVG(CASE WHEN order_value >= 100 THEN 1.0 ELSE 0.0 END) AS pct_orders_above_100
FROM completed_orders
GROUP BY 1
ORDER BY 1 DESC;

-- 2) Last 12 full months: order value bands near the shipping threshold
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
date_context AS (
  SELECT
    CAST(date_trunc('month', MAX(order_ts)) AS DATE) - INTERVAL 1 DAY AS snapshot_date,
    CAST(date_trunc('month', MAX(order_ts)) AS DATE) - INTERVAL 1 YEAR AS trailing_start
  FROM completed_orders
)
SELECT
  CASE
    WHEN order_value < 50 THEN '<$50'
    WHEN order_value < 75 THEN '$50-$74'
    WHEN order_value < 100 THEN '$75-$99'
    WHEN order_value < 125 THEN '$100-$124'
    ELSE '$125+'
  END AS order_band,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT order_id) * 1.0 / SUM(COUNT(DISTINCT order_id)) OVER () AS order_share
FROM completed_orders co
CROSS JOIN date_context dc
WHERE CAST(co.order_ts AS DATE) BETWEEN dc.trailing_start AND dc.snapshot_date
GROUP BY 1
ORDER BY CASE order_band
  WHEN '<$50' THEN 1
  WHEN '$50-$74' THEN 2
  WHEN '$75-$99' THEN 3
  WHEN '$100-$124' THEN 4
  WHEN '$125+' THEN 5
END;

-- 3) First-order repeat within 90 days by acquisition channel
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
ordered AS (
  SELECT
    user_id,
    order_id,
    order_ts,
    order_value,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_ts) AS order_rank,
    LEAD(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts) AS next_order_ts
  FROM completed_orders
)
SELECT
  u.traffic_source,
  COUNT(DISTINCT o.user_id) AS customers,
  AVG(CASE
    WHEN o.next_order_ts IS NOT NULL
         AND o.next_order_ts <= o.order_ts + INTERVAL 90 DAY THEN 1.0
    ELSE 0.0
  END) AS repeat_90d,
  AVG(o.order_value) AS first_order_aov
FROM ordered o
JOIN read_parquet('thelook_parquet_data/users.parquet') u
  ON o.user_id = u.id
WHERE o.order_rank = 1
GROUP BY 1
ORDER BY customers DESC;
