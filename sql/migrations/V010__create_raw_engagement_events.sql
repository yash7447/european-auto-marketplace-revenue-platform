CREATE EXTERNAL TABLE IF NOT EXISTS
auto_marketplace_raw.raw_engagement_events
(
    event_id STRING,
    event_timestamp STRING,
    listing_id STRING,
    dealer_id STRING,
    session_id STRING,
    event_type STRING,
    device STRING
)
PARTITIONED BY
(
    event_date STRING
)
ROW FORMAT SERDE
'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES
(
    'mapping.event_timestamp'='timestamp'
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/raw/engagement/'
