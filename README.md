# thelook Ecommerce – Product Analyst Challenge Submission

This repository contains the complete solution for the Peek Product Analyst 2026 Data Challenge.

- **Part 1** – SQL queries (Tasks A–D) in `analysis/PART_1.ipynb`
- **Part 2** – Analysis & findings presented as a fully static GitHub Pages dashboard at [mandr4k3.github.io/thelook_ecommerce/analysis/dashboard/index.html](https://mandr4k3.github.io/thelook_ecommerce/analysis/dashboard/index.html)

## How to Explore

1. **Part 1 (SQL)**: Open `analysis/PART_1.ipynb` – all four tasks are executed with outputs.
2. **Part 2 (Analysis & Visuals)**: Open the [live dashboard](https://mandr4k3.github.io/thelook_ecommerce/analysis/dashboard/index.html) or review the source in `analysis/dashboard/index.html`.

## Assumptions & Date Ranges
- All queries run on the full `bigquery-public-data.thelook_ecommerce` dataset.
- Start date: `2020-01-01` (parameterized in every query).
- Task D launch date: `2022-01-15` (as specified in the brief).
- 90-day churn calculations are censored using the dataset’s own `MAX(created_at)` for reproducibility.

## Definitions (Exactly as Used in Part 1)

**Active customer**: User with ≥1 completed, non-returned order in the month.  
**New customer**: First-ever completed order occurs in that month.  
**Returning customer**: Any completed order in a month strictly after their first purchase month.  
**90-day churn**: A user-month is churned if the user places zero completed orders in the 90 days after their last order in that month.  
**Monthly Conversion Rate (Task A)**: Distinct `purchase` event users ÷ distinct `department` event users (monthly proxy).

**Alternatives considered** (in dashboard):
- 30-day reorder churn or 14-day session churn for faster signals.
- Cohort-based retention for leadership reporting.

## Part 2 – Visuals & Findings
Presented via the static dashboard: [Live link](https://mandr4k3.github.io/thelook_ecommerce/analysis/dashboard/index.html)  
Source file: `analysis/dashboard/index.html`

- New vs Returning Revenue Mix by Month
- Monthly Churn Rate vs Revenue
- Customer Groups by Size and Revenue
- Free Shipping > $100 experiment design + proxy baseline

All visuals are built directly from the outputs of Part 1 queries.

## AI Usage in This Challenge
I used AI to:
- Review and harden SQL queries for edge cases and reproducibility.
- Create, push, and update this repository through the GitHub MCP alongside local development.
- Accelerate the local Querybook setup, which I used as a query IDE for EDA.
- Add a local Ollama LLM to help optimize queries during analysis.
- Automate table downloads into parquet files after evaluating that Querybook plus scripted parquet extraction would give the fastest response.

**Example prompts used**:
> “Write a Python script that downloads selected tables from `bigquery-public-data.thelook_ecommerce` into local parquet files. Make the project ID configurable, print progress and file sizes.”

> “Help me set up Querybook locally with Docker, then connect its AI feature to a local Ollama deployment using the Gemma 4 26B A4B model so I can use it for query optimization. Keep the setup practical, local-first, and focused on speeding up SQL iteration.”

I validated every AI suggestion by running the queries myself and cross-checking row counts and definitions against the job brief.

## Questions Addressed (Part 2 Requirements)

**1. How were churn, active customer, and new vs returning defined?**  
(See Definitions section above + full alternatives in the dashboard.)

**2. Most important trend for leadership?**  
Returning revenue share is improving (from 1.9% in 2020 to ~15% in the latest 12 months) but the business remains heavily acquisition-led. Retention is the largest untapped growth lever.

**3. One product experiment I would run?**  
Randomized test of **free shipping above $100**, targeted first at newly acquired Search-led customers.  
Primary metric: completed-order conversion rate.  
Secondary: AOV, second-order rate. Guardrails: margin, returns, cancellations.

## Reproducibility
- All Part 1 SQL is fully parameterized and uses only Standard SQL.
- Dashboard data is hardcoded from local parquet extracts (`thelook_parquet_data/`) for zero-dependency GitHub Pages deployment.
- SQL lineage for every dashboard section is in `analysis/dashboard/queries/`.

## Google Cloud Project ID
If you want to rerun the raw table download in `scripts/fetch_thelook_table.py`, replace `PROJECT_ID = "gmp-demo"` with your own Google Cloud Project ID.

Using your own Project ID also lets you run the Part 1 notebook queries against the public dataset from Python, without needing to work directly in the BigQuery UI.

To find it in Google Cloud Console:
- Open the project selector in the top navigation bar.
- Choose your project and copy the value shown as **Project ID**.
- Use the **Project ID**, not the project name or project number.
