MERGE INTO
    auto_marketplace_staging.stg_marketplace_vehicles AS target

USING
(
    SELECT
        vehicle_id,
        make,
        model,
        model_year,
        fuel_type,
        body_type,
        mileage_km,
        created_at,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at

    FROM
    (
        SELECT
            CAST(vehicle_id AS VARCHAR)
                AS vehicle_id,

            CAST(make AS VARCHAR)
                AS make,

            CAST(model AS VARCHAR)
                AS model,

            TRY_CAST(model_year AS INTEGER)
                AS model_year,

            CAST(fuel_type AS VARCHAR)
                AS fuel_type,

            CAST(body_type AS VARCHAR)
                AS body_type,

            TRY_CAST(mileage_km AS INTEGER)
                AS mileage_km,

            TRY_CAST(created_at AS TIMESTAMP)
                AS created_at,

            TRY_CAST(updated_at AS TIMESTAMP)
                AS updated_at,

            TRY_CAST(extract_date AS DATE)
                AS source_extract_date,

            CAST(run_id AS VARCHAR)
                AS source_run_id,

            "$path"
                AS source_file,

            CAST(
                CURRENT_TIMESTAMP AS TIMESTAMP
            ) AS staged_at,

            ROW_NUMBER() OVER
            (
                PARTITION BY vehicle_id

                ORDER BY
                    TRY_CAST(
                        updated_at AS TIMESTAMP
                    ) DESC NULLS LAST,

                    TRY_CAST(
                        extract_date AS DATE
                    ) DESC NULLS LAST,

                    run_id DESC
            ) AS row_number

        FROM
            auto_marketplace_raw.raw_marketplace_vehicles

        WHERE
            __SOURCE_FILTER__

            AND "$path"
                LIKE '%.jsonl.gz'

            AND vehicle_id IS NOT NULL
    )

    WHERE row_number = 1
) AS source

ON
    target.vehicle_id = source.vehicle_id

WHEN MATCHED THEN
    UPDATE SET

        make =
            source.make,

        model =
            source.model,

        model_year =
            source.model_year,

        fuel_type =
            source.fuel_type,

        body_type =
            source.body_type,

        mileage_km =
            source.mileage_km,

        created_at =
            source.created_at,

        updated_at =
            source.updated_at,

        source_extract_date =
            source.source_extract_date,

        source_run_id =
            source.source_run_id,

        source_file =
            source.source_file,

        staged_at =
            source.staged_at

WHEN NOT MATCHED THEN
    INSERT
    (
        vehicle_id,
        make,
        model,
        model_year,
        fuel_type,
        body_type,
        mileage_km,
        created_at,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )

    VALUES
    (
        source.vehicle_id,
        source.make,
        source.model,
        source.model_year,
        source.fuel_type,
        source.body_type,
        source.mileage_km,
        source.created_at,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );