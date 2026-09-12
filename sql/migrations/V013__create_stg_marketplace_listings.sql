CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_marketplace_listings
(
    listing_id string,
    vehicle_id string,
    dealer_id string,

    listing_start_date date,
    listing_end_date date,

    asking_price_eur decimal(12,2),

    listing_status string,
    visibility_package string,

    created_at timestamp,
    updated_at timestamp,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(listing_start_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/marketplace/listings/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
