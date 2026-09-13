MERGE INTO auto_marketplace_staging.stg_pricing_prices AS target
USING
(
    SELECT
        price_id,
        product_id,
        market,
        dealer_segment,
        currency,
        list_price,
        effective_from,
        effective_to,
        pricing_version,
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
                PARTITION BY price_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.price_id') AS VARCHAR) AS price_id,
                CAST(json_extract_scalar(record, '$.product_id') AS VARCHAR) AS product_id,
                CAST(json_extract_scalar(record, '$.market') AS VARCHAR) AS market,
                CAST(json_extract_scalar(record, '$.dealer_segment') AS VARCHAR) AS dealer_segment,
                CAST(json_extract_scalar(record, '$.currency') AS VARCHAR) AS currency,
                TRY_CAST(json_extract_scalar(record, '$.list_price') AS DECIMAL(12,2)) AS list_price,
                TRY_CAST(json_extract_scalar(record, '$.effective_from') AS DATE) AS effective_from,
                TRY_CAST(json_extract_scalar(record, '$.effective_to') AS DATE) AS effective_to,
                CAST(json_extract_scalar(record, '$.pricing_version') AS VARCHAR) AS pricing_version,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_pricing_prices AS r
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
        WHERE price_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.price_id = source.price_id
WHEN MATCHED THEN
    UPDATE SET
        product_id = source.product_id,
        market = source.market,
        dealer_segment = source.dealer_segment,
        currency = source.currency,
        list_price = source.list_price,
        effective_from = source.effective_from,
        effective_to = source.effective_to,
        pricing_version = source.pricing_version,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        price_id,
        product_id,
        market,
        dealer_segment,
        currency,
        list_price,
        effective_from,
        effective_to,
        pricing_version,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.price_id,
        source.product_id,
        source.market,
        source.dealer_segment,
        source.currency,
        source.list_price,
        source.effective_from,
        source.effective_to,
        source.pricing_version,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
