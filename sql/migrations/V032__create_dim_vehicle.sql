CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.dim_vehicle (
    vehicle_key string,
    vehicle_id string,
    make string,
    model string,
    model_year int,
    fuel_type string,
    body_type string,
    mileage_km int,
    created_at timestamp,
    updated_at timestamp,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/dimensions/dim_vehicle/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
