CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.dim_product (
    product_key string,
    product_id string,
    product_name string,
    product_family string,
    billing_frequency string,
    active boolean,
    updated_at date,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/dimensions/dim_product/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
