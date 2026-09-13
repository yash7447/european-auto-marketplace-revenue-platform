CREATE TABLE IF NOT EXISTS auto_marketplace_intermediate.int_sales_targets (
    target_id string,
    target_month date,
    market string,
    sales_rep string,
    product_group string,
    revenue_target_eur decimal(14,2),
    upsell_target_eur decimal(14,2),
    new_dealer_target int,
    retention_target_pct decimal(7,4),
    planning_version string,
    created_at timestamp,
    transformed_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/intermediate/int_sales_targets/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
