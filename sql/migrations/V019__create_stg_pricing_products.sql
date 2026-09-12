CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_pricing_products
(
    product_id string,
    product_name string,
    product_family string,

    billing_frequency string,

    active boolean,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/pricing/products/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
