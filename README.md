# thelook_ecommerce BigQuery downloader

This repo contains a small Python CLI for pulling tables from the public BigQuery dataset:

- `bigquery-public-data.thelook_ecommerce`

The Google Cloud Console URL you shared points to the dataset root, not one specific table, so the script supports both:

- listing the available tables
- downloading any chosen table to CSV or JSONL

## 1. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## 2. Authenticate locally

Use Application Default Credentials:

```bash
gcloud auth application-default login
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
```

If you prefer a service account, set:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
```

## 3. List the dataset tables

```bash
python3 scripts/fetch_thelook_table.py list-tables
```

## 4. Download a table

Download a safe sample first:

```bash
python3 scripts/fetch_thelook_table.py download orders --limit 1000 --output data/orders.csv
```

Download the full table:

```bash
python3 scripts/fetch_thelook_table.py download orders --all --output data/orders.csv
```

Download JSONL instead of CSV:

```bash
python3 scripts/fetch_thelook_table.py download users --format jsonl --output data/users.jsonl
```

## Notes

- `--billing-project` can be passed explicitly if you do not want to rely on `GOOGLE_CLOUD_PROJECT`.
- By default, `download` only fetches `1000` rows so you do not accidentally pull a large table.
- Pass `--overwrite` if the output file already exists.
