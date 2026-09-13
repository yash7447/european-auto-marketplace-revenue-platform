CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.mart_marketplace_funnel_monthly (
    funnel_key string,
    month_start date,
    market string,
    views bigint,
    clicks bigint,
    leads bigint,
    unique_sessions bigint,
    listings_with_activity bigint,
    dealers_with_activity bigint,
    click_rate_pct decimal(12,4),
    lead_rate_pct decimal(12,4),
    refreshed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/marts/mart_marketplace_funnel_monthly/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
