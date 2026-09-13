MERGE INTO
    auto_marketplace_staging.stg_marketplace_listings AS target

USING
(
    SELECT
        listing_id,
        vehicle_id,
        dealer_id,
        listing_start_date,
        listing_end_date,
        asking_price_eur,
        listing_status,
        visibility_package,
        created_at,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at

    FROM
    (
        SELECT
            CAST(listing_id AS VARCHAR)
                AS listing_id,

            CAST(vehicle_id AS VARCHAR)
                AS vehicle_id,

            CAST(dealer_id AS VARCHAR)
                AS dealer_id,

            TRY_CAST(
                listing_start_date AS DATE
            ) AS listing_start_date,

            TRY_CAST(
                listing_end_date AS DATE
            ) AS listing_end_date,

            TRY_CAST(
                asking_price_eur
                AS DECIMAL(12,2)
            ) AS asking_price_eur,

            CAST(
                listing_status AS VARCHAR
            ) AS listing_status,

            CAST(
                visibility_package AS VARCHAR
            ) AS visibility_package,

            TRY_CAST(
                created_at AS TIMESTAMP
            ) AS created_at,

            TRY_CAST(
                updated_at AS TIMESTAMP
            ) AS updated_at,

            TRY_CAST(
                extract_date AS DATE
            ) AS source_extract_date,

            CAST(
                run_id AS VARCHAR
            ) AS source_run_id,

            "$path"
                AS source_file,

            CAST(
                CURRENT_TIMESTAMP AS TIMESTAMP
            ) AS staged_at,

            ROW_NUMBER() OVER
            (
                PARTITION BY listing_id

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
            auto_marketplace_raw.raw_marketplace_listings

        WHERE
            __SOURCE_FILTER__

            AND "$path"
                LIKE '%.jsonl.gz'

            AND listing_id IS NOT NULL
    )

    WHERE row_number = 1
) AS source

ON
    target.listing_id = source.listing_id

WHEN MATCHED THEN
    UPDATE SET

        vehicle_id =
            source.vehicle_id,

        dealer_id =
            source.dealer_id,

        listing_start_date =
            source.listing_start_date,

        listing_end_date =
            source.listing_end_date,

        asking_price_eur =
            source.asking_price_eur,

        listing_status =
            source.listing_status,

        visibility_package =
            source.visibility_package,

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
        listing_id,
        vehicle_id,
        dealer_id,
        listing_start_date,
        listing_end_date,
        asking_price_eur,
        listing_status,
        visibility_package,
        created_at,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )

    VALUES
    (
        source.listing_id,
        source.vehicle_id,
        source.dealer_id,
        source.listing_start_date,
        source.listing_end_date,
        source.asking_price_eur,
        source.listing_status,
        source.visibility_package,
        source.created_at,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );