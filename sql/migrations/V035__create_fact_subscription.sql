CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.fact_subscription (
    subscription_id string,
    dealer_key string,
    product_key string,
    dealer_id string,
    product_id string,
    contract_id string,
    market string,
    segment string,
    sales_rep string,
    product_family string,
    contract_price decimal(12,2),
    discount_pct decimal(7,4),
    billing_frequency string,
    subscription_status string,
    contract_status string,
    auto_renew boolean,
    subscription_start_date date,
    subscription_end_date date,
    start_date_key int,
    end_date_key int,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/facts/fact_subscription/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
