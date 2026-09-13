MERGE INTO auto_marketplace_analytics.dim_vehicle t
USING (
    SELECT
        to_hex(md5(to_utf8(vehicle_id))) AS vehicle_key,
        vehicle_id,
        make,
        model,
        model_year,
        fuel_type,
        body_type,
        mileage_km,
        created_at,
        updated_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_staging.stg_marketplace_vehicles
) s
ON t.vehicle_id = s.vehicle_id
WHEN MATCHED THEN UPDATE SET
    vehicle_key=s.vehicle_key,
    make=s.make,
    model=s.model,
    model_year=s.model_year,
    fuel_type=s.fuel_type,
    body_type=s.body_type,
    mileage_km=s.mileage_km,
    created_at=s.created_at,
    updated_at=s.updated_at,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    vehicle_key, vehicle_id, make, model, model_year, fuel_type,
    body_type, mileage_km, created_at, updated_at, loaded_at
) VALUES (
    s.vehicle_key, s.vehicle_id, s.make, s.model, s.model_year, s.fuel_type,
    s.body_type, s.mileage_km, s.created_at, s.updated_at, s.loaded_at
);
