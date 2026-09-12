CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_subscriptions
(
    subscription_id string,
    contract_id string,
    dealer_id string,

    product_id string,

    subscription_start_date date,
    subscription_end_date date,

    contract_price decimal(12,2),
    discount_pct decimal(7,4),

    billing_frequency string,
    subscription_status string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(subscription_start_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/subscriptions/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
