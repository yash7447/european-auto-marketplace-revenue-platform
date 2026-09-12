CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_engagement_events
(
    event_id string,

    event_timestamp timestamp,
    event_date date,

    listing_id string,
    dealer_id string,
    session_id string,

    event_type string,
    device string,

    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    event_date
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/engagement/events/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
