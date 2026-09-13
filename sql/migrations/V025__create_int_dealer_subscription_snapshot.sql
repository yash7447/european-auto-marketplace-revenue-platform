CREATE TABLE IF NOT EXISTS auto_marketplace_intermediate.int_dealer_subscription_snapshot (
    subscription_id string,
    contract_id string,
    dealer_id string,
    dealer_name string,
    market string,
    segment string,
    sales_rep string,
    account_status string,
    product_id string,
    product_name string,
    product_family string,
    billing_frequency string,
    subscription_start_date date,
    subscription_end_date date,
    contract_start_date date,
    contract_end_date date,
    contract_price decimal(12,2),
    discount_pct decimal(7,4),
    subscription_status string,
    contract_status string,
    auto_renew boolean,
    account_updated_at date,
    subscription_updated_at date,
    contract_updated_at date,
    product_updated_at date,
    transformed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/intermediate/int_dealer_subscription_snapshot/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
