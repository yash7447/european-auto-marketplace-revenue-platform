CREATE TABLE IF NOT EXISTS auto_marketplace_intermediate.int_invoice_enriched (
    invoice_id string,
    invoice_date date,
    invoice_amount decimal(14,2),
    currency string,
    payment_status string,
    subscription_id string,
    contract_id string,
    dealer_id string,
    dealer_name string,
    market string,
    segment string,
    sales_rep string,
    product_id string,
    product_name string,
    product_family string,
    billing_frequency string,
    contract_price decimal(12,2),
    discount_pct decimal(7,4),
    subscription_status string,
    contract_status string,
    transformed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/intermediate/int_invoice_enriched/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
