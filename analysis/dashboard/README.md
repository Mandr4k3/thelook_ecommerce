# thelook static dashboard

This folder contains a fully static dashboard for GitHub Pages built from the local parquet extracts in `thelook_parquet_data/`.

## Files

- `index.html`: deployable dashboard with hardcoded visuals and narrative.
- `data.js`: hardcoded metric payload embedded by the page.
- `queries/`: SQL lineage used for each dashboard section.

## Questions addressed

### 1. How were churn, active customer, and new vs returning defined?

- `Active customer`: a user with at least one completed, non-returned order in a month.
- `New customer`: a user whose first-ever completed order lands in that month.
- `Returning customer`: a user who orders in any later month after their first purchase month.
- `90-day churn`: a user-month is churned if the user places no completed order in the 90 days after the last completed order in that month.

Alternative definitions I would consider:

- `30-day reorder churn` for faster commerce lifecycle operations.
- `14-day session churn` if product teams need earlier behavioral warning signals.
- `Cohort retention` for more stable retention reporting to leadership.

### 2. What is the most important trend for leadership?

The single most important trend is that returning revenue share is improving, but the business is still acquisition-led.

- Returning revenue share rose from `1.9%` in the 2020 monthly average to `15.0%` across the latest 12 full months.
- The latest full month still generated only `14.1%` of revenue from returning customers.
- Monthly revenue and 90-day churn have a `-0.778` correlation, so improving repeat behavior is directly tied to monetization.

Leadership implication: the growth model is still mostly “fill the funnel again next month,” not “grow a healthy repeat base.”

### 3. If I could run one product experiment, what would it be?

I would run a randomized test of `free shipping above $100`.

- Target segment: newly acquired customers first, especially Search-led traffic because it is the largest acquisition source and first-order repeat within 90 days is still only about `3.1%`.
- Why this test: `10.4%` of last-12-month orders already land in the `$75-$99` band, which means there is a real population close enough to the threshold to be nudged upward.

Success metrics:

- Primary: completed-order conversion rate.
- Secondary: average order value, revenue per visitor, share of orders at or above `$100`, and second-order rate.
- Guardrails: shipping cost per order, gross margin, return rate, and cancellation rate.

## Segmenting logic

The dashboard uses a simple RFM-style segmentation as of `2026-03-31`:

- `Champions`: purchased in the last 90 days, at least 3 orders, and above the 75th percentile for lifetime revenue.
- `Active repeat`: purchased in the last 90 days with at least 2 lifetime orders.
- `Newly acquired`: first and only purchase inside the last 90 days.
- `At risk`: last purchase 91 to 180 days ago.
- `Dormant`: last purchase more than 180 days ago.

This split is intentionally operational:

- protect `Active repeat` and `Champions`,
- reactivate `At risk`,
- redesign the first-to-second purchase path for `Newly acquired`.

## Query notes

- Queries are written against the local parquet extracts using DuckDB-style `read_parquet(...)`.
- Paths assume execution from the repo root.
- The experiment section uses more than one query because it combines baseline proxy metrics, order-value banding, and first-order repeat by acquisition channel.
