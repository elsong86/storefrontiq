# StorefrontIQ

A data engineering pipeline that delivers weekly customer sentiment 
reports to small business owners. Built to practice medallion 
architecture, lakehouse patterns, and cloud data warehousing.

## Architecture

Bronze (raw parquet in S3) → Silver (cleaned/enriched parquet in S3) 
→ Gold (aggregated tables in Snowflake) → Weekly email reports

## Tech Stack

- **Ingestion:** Python, Outscraper API, Google Places API
- **Storage:** AWS S3 with Delta Lake (ACID compliance)
- **Processing:** PySpark / pandas, spaCy, VADER
- **Warehouse:** Snowflake
- **Orchestration:** Apache Airflow
- **Delivery:** FastAPI + SendGrid

## Project Status

🚧 In active development. See [roadmap](#roadmap) below.

## Background

Originally built as "Taco About It," a consumer iOS app for finding 
highly-rated taco spots. Pivoted into a B2B data product to explore 
medallion architecture and lakehouse patterns end-to-end. 
Legacy code preserved in `/archive`.

## Roadmap

- [ ] Phase 1: Bronze layer — S3 + Delta Lake ingestion
- [ ] Phase 2: Silver layer — sentiment transforms
- [ ] Phase 3: Gold layer — Snowflake modeling
- [ ] Phase 4: Airflow orchestration
- [ ] Phase 5: Signup + weekly email delivery