CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.dim_dealer (
    dealer_key string,
    dealer_id string,
    dealer_name string,
    market string,
    segment string,
    sales_rep string,
    account_status string,
    created_date date,
    updated_at date,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/dimensions/dim_dealer/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
