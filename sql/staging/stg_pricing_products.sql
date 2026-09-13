MERGE INTO auto_marketplace_staging.stg_pricing_products AS target
USING
(
    SELECT
        product_id,
        product_name,
        product_family,
        billing_frequency,
        active,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    FROM
    (
        SELECT
            normalized.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY product_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.product_id') AS VARCHAR) AS product_id,
                CAST(json_extract_scalar(record, '$.product_name') AS VARCHAR) AS product_name,
                CAST(json_extract_scalar(record, '$.product_family') AS VARCHAR) AS product_family,
                CAST(json_extract_scalar(record, '$.billing_frequency') AS VARCHAR) AS billing_frequency,
                TRY_CAST(json_extract_scalar(record, '$.active') AS BOOLEAN) AS active,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_pricing_products AS r
            CROSS JOIN UNNEST
            (
                CAST(
                    json_extract(r.raw_json, '$.data')
                    AS ARRAY(JSON)
                )
            ) AS u(record)
            WHERE
                __SOURCE_FILTER__
                AND "$path" LIKE '%page=%.json'
        ) AS normalized
        WHERE product_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET
        product_name = source.product_name,
        product_family = source.product_family,
        billing_frequency = source.billing_frequency,
        active = source.active,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        product_id,
        product_name,
        product_family,
        billing_frequency,
        active,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.product_id,
        source.product_name,
        source.product_family,
        source.billing_frequency,
        source.active,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
