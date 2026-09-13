CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.mart_dealer_360 (
    dealer_id string,
    dealer_key string,
    dealer_name string,
    market string,
    segment string,
    sales_rep string,
    account_status string,
    lifetime_revenue_eur decimal(18,2),
    invoice_count bigint,
    active_subscriptions bigint,
    product_count bigint,
    avg_discount_pct decimal(12,4),
    total_listings bigint,
    active_listings bigint,
    views bigint,
    clicks bigint,
    leads bigint,
    lead_rate_pct decimal(12,4),
    last_invoice_date date,
    last_engagement_at timestamp,
    refreshed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/marts/mart_dealer_360/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
