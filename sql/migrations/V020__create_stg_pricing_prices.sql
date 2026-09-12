CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_pricing_prices
(
    price_id string,
    product_id string,

    market string,
    dealer_segment string,

    currency string,

    list_price decimal(12,2),

    effective_from date,
    effective_to date,

    pricing_version string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/pricing/prices/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
