SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_marketplace_listings l
LEFT JOIN auto_marketplace_staging.stg_marketplace_vehicles v
    ON l.vehicle_id = v.vehicle_id
WHERE
    l.vehicle_id IS NOT NULL
    AND v.vehicle_id IS NULL;
