CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_marketplace_vehicles
(
    vehicle_id string,

    make string,
    model string,

    model_year int,

    fuel_type string,
    body_type string,

    mileage_km int,

    created_at timestamp,
    updated_at timestamp,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/marketplace/vehicles/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
