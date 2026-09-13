CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.mart_product_performance_monthly (
    product_month_key string,
    month_start date,
    market string,
    product_id string,
    product_name string,
    product_family string,
    revenue_eur decimal(18,2),
    invoice_count bigint,
    dealer_count bigint,
    active_subscriptions bigint,
    avg_contract_price decimal(14,2),
    avg_discount_pct decimal(12,4),
    avg_list_price decimal(14,2),
    revenue_per_dealer decimal(18,2),
    refreshed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/marts/mart_product_performance_monthly/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
