CREATE EXTERNAL TABLE IF NOT EXISTS
auto_marketplace_raw.raw_marketplace_vehicles
(
    vehicle_id  STRING,
    make        STRING,
    model       STRING,
    model_year  INT,
    fuel_type   STRING,
    body_type   STRING,
    mileage_km  INT,
    created_at  STRING,
    updated_at  STRING
)
PARTITIONED BY
(
    extract_date STRING,
    run_id STRING
)
ROW FORMAT SERDE
'org.openx.data.jsonserde.JsonSerDe'
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/raw/marketplace/vehicles/'
