/* ===========================================================================
FILE: part1_queries.sql
DATASET: bigquery-public-data.thelook_ecommerce
===========================================================================
*/

/* ===========================================================================
TASK A: MONTHLY PRODUCT METRICS
===========================================================================
ASSUMPTIONS & DEFINITIONS:
1. Parameterization: Date variables are handled via a params CTE for full 
   compatibility with BI tools and standard Views.
2. Conversion Rate Proxy: Defined as:
   (Distinct users with 'purchase' event in month X) / 
   (Distinct users with 'department' event in month X)
   Limitation: This is a monthly user-level proxy. It does not guarantee 
   intra-month sequence and is not strictly session-based.
===========================================================================
*/

WITH query_params AS (
    SELECT 
        DATE('2020-01-01') AS start_date,
        DATE('2024-12-31') AS end_date
),
completed_sales AS (
  SELECT
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS month,
    oi.order_id,
    oi.id AS order_item_id,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  CROSS JOIN query_params qp
  WHERE oi.status = 'Complete'
    AND oi.returned_at IS NULL
    AND DATE(oi.created_at) BETWEEN qp.start_date AND qp.end_date
),
sales_metrics AS (
  SELECT
    month,
    SUM(sale_price) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(order_item_id) AS units,
    SAFE_DIVIDE(SUM(sale_price), COUNT(DISTINCT order_id)) AS aov
  FROM completed_sales
  GROUP BY 1
),
event_metrics AS (
  SELECT
    DATE_TRUNC(DATE(e.created_at), MONTH) AS month,
    COUNT(DISTINCT CASE WHEN e.event_type = 'department' THEN e.user_id END) AS department_users,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.user_id END) AS purchase_users
  FROM `bigquery-public-data.thelook_ecommerce.events` e
  CROSS JOIN query_params qp
  WHERE DATE(e.created_at) BETWEEN qp.start_date AND qp.end_date
  GROUP BY 1
)

SELECT
  s.month,
  s.revenue,
  s.orders,
  s.units,
  s.aov,
  SAFE_DIVIDE(e.purchase_users, e.department_users) AS monthly_conversion_rate
FROM sales_metrics s
LEFT JOIN event_metrics e
  ON s.month = e.month
ORDER BY s.month;


/*
===========================================================================
TASK B: NEW VS RETURNING MIX
===========================================================================
DEFINITIONS:
1. Active Customer: A customer with >= 1 completed order in that month.
2. New Customer: A customer whose first-ever completed order occurs in 
   that specific month.
3. Returning Customer: A customer who makes a completed order in a month 
   strictly AFTER their first-ever completed order month.
===========================================================================
*/

WITH query_params AS (
    SELECT 
        DATE('2020-01-01') AS start_date,
        DATE('2024-12-31') AS end_date
),
completed_sales AS (
  SELECT
    oi.user_id,
    oi.order_id,
    DATE(oi.created_at) AS order_date,
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS order_month,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  CROSS JOIN query_params qp
  WHERE oi.status = 'Complete'
    AND oi.returned_at IS NULL
    AND DATE(oi.created_at) BETWEEN qp.start_date AND qp.end_date
),
first_purchase AS (
  SELECT
    user_id,
    DATE_TRUNC(MIN(order_date), MONTH) AS first_purchase_month
  FROM completed_sales
  GROUP BY 1
),
monthly_user_sales AS (
  SELECT
    user_id,
    order_month,
    SUM(sale_price) AS monthly_user_revenue
  FROM completed_sales
  GROUP BY 1, 2
)

SELECT
  mus.order_month AS month,
  COUNT(DISTINCT mus.user_id) AS active_customers,
  COUNT(DISTINCT CASE WHEN mus.order_month = fp.first_purchase_month THEN mus.user_id END) AS new_customers,
  COUNT(DISTINCT CASE WHEN mus.order_month > fp.first_purchase_month THEN mus.user_id END) AS returning_customers,
  SUM(CASE WHEN mus.order_month = fp.first_purchase_month THEN mus.monthly_user_revenue ELSE 0 END) AS revenue_new,
  SUM(CASE WHEN mus.order_month > fp.first_purchase_month THEN mus.monthly_user_revenue ELSE 0 END) AS revenue_returning,
  SAFE_DIVIDE(
    SUM(CASE WHEN mus.order_month > fp.first_purchase_month THEN mus.monthly_user_revenue ELSE 0 END),
    SUM(mus.monthly_user_revenue)
  ) AS pct_revenue_from_returning
FROM monthly_user_sales mus
JOIN first_purchase fp ON mus.user_id = fp.user_id
GROUP BY 1
ORDER BY 1;


/*
===========================================================================
TASK C: 90-DAY CHURN
===========================================================================
DEFINITIONS & LIMITATIONS:
1. Churn Definition: A user is considered churned for a given active month 
   if they have ZERO completed orders in the 90 days following their LAST 
   order in that specific month.
2. Reproducibility / Dataset Boundary: To ensure historical accuracy, we 
   calculate the global MAX(created_at) of the dataset rather than using 
   CURRENT_DATE(). This safely censors recent months that have not had a 
   full 90 days to mature within the dataset boundaries.
===========================================================================
*/

WITH query_params AS (
    SELECT 
        DATE('2020-01-01') AS start_date,
        DATE('2024-12-31') AS end_date
),
completed_sales AS (
  SELECT
    oi.user_id,
    oi.order_id,
    DATE(oi.created_at) AS order_date,
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS active_month
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  CROSS JOIN query_params qp
  WHERE oi.status = 'Complete'
    AND oi.returned_at IS NULL
    AND DATE(oi.created_at) BETWEEN qp.start_date AND qp.end_date
),
data_boundary AS (
  SELECT MAX(order_date) AS max_observed_order_date
  FROM completed_sales
),
user_month_activity AS (
  SELECT
    user_id,
    active_month,
    MAX(order_date) AS last_order_date_in_month
  FROM completed_sales
  GROUP BY 1, 2
),
eligible_user_months AS (
  SELECT
    uma.user_id,
    uma.active_month,
    uma.last_order_date_in_month
  FROM user_month_activity uma
  CROSS JOIN data_boundary db
  WHERE DATE_ADD(uma.last_order_date_in_month, INTERVAL 90 DAY) <= db.max_observed_order_date
),
user_future_orders AS (
  SELECT
    eum.user_id,
    eum.active_month,
    COUNT(DISTINCT cs.order_id) AS orders_in_next_90d
  FROM eligible_user_months eum
  LEFT JOIN completed_sales cs
    ON eum.user_id = cs.user_id
   AND cs.order_date > eum.last_order_date_in_month
   AND cs.order_date <= DATE_ADD(eum.last_order_date_in_month, INTERVAL 90 DAY)
  GROUP BY 1, 2
)

SELECT
  active_month AS month,
  COUNT(DISTINCT user_id) AS active_customers,
  COUNT(DISTINCT CASE WHEN orders_in_next_90d = 0 THEN user_id END) AS churned_customers_90d,
  SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN orders_in_next_90d = 0 THEN user_id END),
    COUNT(DISTINCT user_id)
  ) AS churn_rate_90d
FROM user_future_orders
GROUP BY 1
ORDER BY 1;


/*
===========================================================================
TASK D: PRODUCT CHANGE IMPACT ANALYSIS (FREE SHIPPING > $100)
===========================================================================
ASSUMPTIONS:
1. Feature Rollout: Assumes 100% rollout on launch date.
2. Cart Value Proxy: Assumes SUM(sale_price) per order accurately reflects 
   the pre-tax cart value.
3. Parameterization: Handled via CTE to ensure the query remains a standard 
   single SQL statement compatible with visualization tools.
===========================================================================
*/

WITH scenario_params AS (
    SELECT 
        DATE('2022-01-15') AS launch_date,
        DATE('2021-12-16') AS pre_window_start,
        DATE('2022-02-14') AS post_window_end
),
completed_sales AS (
  SELECT
    oi.order_id,
    oi.user_id,
    DATE(oi.created_at) AS order_date,
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS order_month,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  CROSS JOIN scenario_params sp
  WHERE oi.status = 'Complete'
    AND oi.returned_at IS NULL
    AND DATE(oi.created_at) BETWEEN sp.pre_window_start AND sp.post_window_end
),
order_totals AS (
  SELECT
    cs.order_id,
    cs.user_id,
    cs.order_date,
    cs.order_month,
    SUM(cs.sale_price) AS order_value
  FROM completed_sales cs
  GROUP BY 1, 2, 3, 4
),
orders_enriched AS (
  SELECT
    ot.order_id,
    ot.order_month,
    ot.order_date,
    u.traffic_source,
    ot.order_value,
    CASE
      WHEN ot.order_date < sp.launch_date THEN 'Pre-Launch'
      ELSE 'Post-Launch'
    END AS period,
    CASE
      WHEN ot.order_value >= 100 THEN 'At/Above $100'
      ELSE 'Below $100'
    END AS threshold_segment
  FROM order_totals ot
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON ot.user_id = u.id
  CROSS JOIN scenario_params sp
)

SELECT
  order_month,
  period,
  traffic_source,
  threshold_segment,
  COUNT(DISTINCT order_id) AS completed_orders,
  SUM(order_value) AS total_revenue,
  SAFE_DIVIDE(SUM(order_value), COUNT(DISTINCT order_id)) AS aov
FROM orders_enriched
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;