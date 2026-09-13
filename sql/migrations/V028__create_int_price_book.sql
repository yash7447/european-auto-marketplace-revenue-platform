CREATE TABLE IF NOT EXISTS auto_marketplace_intermediate.int_price_book (
    price_id string,
    product_id string,
    product_name string,
    product_family string,
    market string,
    dealer_segment string,
    currency string,
    list_price decimal(12,2),
    effective_from date,
    effective_to date,
    pricing_version string,
    product_active boolean,
    transformed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/intermediate/int_price_book/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
