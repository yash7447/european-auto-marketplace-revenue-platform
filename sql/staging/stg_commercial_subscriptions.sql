MERGE INTO auto_marketplace_staging.stg_commercial_subscriptions AS target
USING
(
    SELECT
        subscription_id,
        contract_id,
        dealer_id,
        product_id,
        subscription_start_date,
        subscription_end_date,
        contract_price,
        discount_pct,
        billing_frequency,
        subscription_status,
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
                PARTITION BY subscription_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.subscription_id') AS VARCHAR) AS subscription_id,
                CAST(json_extract_scalar(record, '$.contract_id') AS VARCHAR) AS contract_id,
                CAST(json_extract_scalar(record, '$.dealer_id') AS VARCHAR) AS dealer_id,
                CAST(json_extract_scalar(record, '$.product_id') AS VARCHAR) AS product_id,
                TRY_CAST(json_extract_scalar(record, '$.subscription_start_date') AS DATE) AS subscription_start_date,
                TRY_CAST(json_extract_scalar(record, '$.subscription_end_date') AS DATE) AS subscription_end_date,
                TRY_CAST(json_extract_scalar(record, '$.contract_price') AS DECIMAL(12,2)) AS contract_price,
                TRY_CAST(json_extract_scalar(record, '$.discount_pct') AS DECIMAL(7,4)) AS discount_pct,
                CAST(json_extract_scalar(record, '$.billing_frequency') AS VARCHAR) AS billing_frequency,
                CAST(json_extract_scalar(record, '$.subscription_status') AS VARCHAR) AS subscription_status,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_commercial_subscriptions AS r
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
        WHERE subscription_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.subscription_id = source.subscription_id
WHEN MATCHED THEN
    UPDATE SET
        contract_id = source.contract_id,
        dealer_id = source.dealer_id,
        product_id = source.product_id,
        subscription_start_date = source.subscription_start_date,
        subscription_end_date = source.subscription_end_date,
        contract_price = source.contract_price,
        discount_pct = source.discount_pct,
        billing_frequency = source.billing_frequency,
        subscription_status = source.subscription_status,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        subscription_id,
        contract_id,
        dealer_id,
        product_id,
        subscription_start_date,
        subscription_end_date,
        contract_price,
        discount_pct,
        billing_frequency,
        subscription_status,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.subscription_id,
        source.contract_id,
        source.dealer_id,
        source.product_id,
        source.subscription_start_date,
        source.subscription_end_date,
        source.contract_price,
        source.discount_pct,
        source.billing_frequency,
        source.subscription_status,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
