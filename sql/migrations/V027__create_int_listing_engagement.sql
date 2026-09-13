CREATE TABLE IF NOT EXISTS auto_marketplace_intermediate.int_listing_engagement (
    listing_id string,
    vehicle_id string,
    dealer_id string,
    listing_start_date date,
    listing_end_date date,
    asking_price_eur decimal(12,2),
    listing_status string,
    visibility_package string,
    make string,
    model string,
    model_year int,
    fuel_type string,
    body_type string,
    mileage_km int,
    first_event_at timestamp,
    last_event_at timestamp,
    views bigint,
    clicks bigint,
    leads bigint,
    engagement_events bigint,
    unique_sessions bigint,
    transformed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/intermediate/int_listing_engagement/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
