CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_contracts
(
    contract_id string,
    dealer_id string,

    contract_start_date date,
    contract_end_date date,

    contract_status string,

    auto_renew boolean,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/contracts/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
