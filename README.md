# Sales ETL Pipeline

End-to-end ELT pipeline built with AWS S3, Snowflake, dbt, Airflow and GitHub Actions.

## Architecture
```
Client S3 (client-landing/)
        ↓ Python boto3
Your S3 (landing/)
        ↓ Python load_bronze.py
Snowflake Bronze (raw tables)
        ↓ dbt Silver models
Snowflake Silver (cleaned tables)
        ↓ dbt Gold models
Snowflake Gold (star schema)
```

## Tech Stack

| Tool | Purpose |
|---|---|
| AWS S3 | Raw data landing zone |
| Python + boto3 | File ingestion from S3 |
| Snowflake | Cloud data warehouse |
| dbt | ELT transformations |
| Airflow | Pipeline orchestration |
| GitHub Actions | CI/CD |

## Medallion Architecture

| Layer | Schema | Description |
|---|---|---|
| Bronze | ecommerce_dw.bronze | Raw data as-is from source |
| Silver | ecommerce_dw.silver | Cleaned + standardised |
| Gold | ecommerce_dw.gold | Star schema for reporting |

## Data Sources

| Source | System | Tables |
|---|---|---|
| Customer info | CRM | crm_cust_info, crm_cust_az12, crm_loc_a101 |
| Product info | ERP | erp_prd_info, erp_px_cat_g1v2 |
| Sales data | ERP | erp_sales_details |

## Gold Layer — Star Schema
```
fct_sales
    ├── dim_customers  (customer_key)
    ├── dim_products   (product_key)
    └── dim_date       (order_date_key)
```

## How to Run

1. Upload CSV files to S3 client-landing/
2. Run ingestion: `python3 scripts/ingest_from_s3.py`
3. Load Bronze: `python3 scripts/load_bronze.py`
4. Transform: `cd dbt && dbt build`
