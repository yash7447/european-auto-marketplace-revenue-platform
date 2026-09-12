CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_accounts
(
    dealer_id string,
    dealer_name string,

    market string,
    segment string,

    sales_rep string,

    account_status string,

    created_date date,
    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/accounts/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
