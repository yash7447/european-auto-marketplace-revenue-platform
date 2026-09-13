CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.fact_revenue (
    invoice_id string,
    invoice_date date,
    date_key int,
    dealer_key string,
    product_key string,
    dealer_id string,
    product_id string,
    subscription_id string,
    contract_id string,
    market string,
    segment string,
    sales_rep string,
    product_family string,
    invoice_amount decimal(14,2),
    currency string,
    payment_status string,
    contract_price decimal(12,2),
    discount_pct decimal(7,4),
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/facts/fact_revenue/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
