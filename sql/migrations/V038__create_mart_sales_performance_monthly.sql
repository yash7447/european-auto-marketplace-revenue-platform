CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.mart_sales_performance_monthly (
    performance_key string,
    month_start date,
    market string,
    sales_rep string,
    product_group string,
    revenue_eur decimal(18,2),
    revenue_target_eur decimal(18,2),
    upsell_target_eur decimal(18,2),
    new_dealer_target bigint,
    retention_target_pct decimal(7,4),
    attainment_pct decimal(12,4),
    invoice_count bigint,
    revenue_dealer_count bigint,
    active_subscriptions bigint,
    refreshed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/marts/mart_sales_performance_monthly/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
