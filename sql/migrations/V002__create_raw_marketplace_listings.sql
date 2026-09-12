CREATE EXTERNAL TABLE IF NOT EXISTS
auto_marketplace_raw.raw_marketplace_listings
(
    listing_id          STRING,
    vehicle_id          STRING,
    dealer_id           STRING,
    listing_start_date  STRING,
    listing_end_date    STRING,
    asking_price_eur    DOUBLE,
    listing_status      STRING,
    visibility_package  STRING,
    created_at          STRING,
    updated_at          STRING
)
PARTITIONED BY
(
    extract_date STRING,
    run_id STRING
)
ROW FORMAT SERDE
'org.openx.data.jsonserde.JsonSerDe'
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/raw/marketplace/listings/'
