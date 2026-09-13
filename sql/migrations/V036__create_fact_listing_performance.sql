CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.fact_listing_performance (
    listing_id string,
    dealer_key string,
    vehicle_key string,
    dealer_id string,
    vehicle_id string,
    listing_start_date date,
    listing_end_date date,
    start_date_key int,
    end_date_key int,
    asking_price_eur decimal(12,2),
    listing_status string,
    visibility_package string,
    views bigint,
    clicks bigint,
    leads bigint,
    engagement_events bigint,
    unique_sessions bigint,
    first_event_at timestamp,
    last_event_at timestamp,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/facts/fact_listing_performance/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
