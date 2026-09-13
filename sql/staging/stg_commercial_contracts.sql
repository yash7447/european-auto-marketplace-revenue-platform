MERGE INTO auto_marketplace_staging.stg_commercial_contracts AS target
USING
(
    SELECT
        contract_id,
        dealer_id,
        contract_start_date,
        contract_end_date,
        contract_status,
        auto_renew,
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
                PARTITION BY contract_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.contract_id') AS VARCHAR) AS contract_id,
                CAST(json_extract_scalar(record, '$.dealer_id') AS VARCHAR) AS dealer_id,
                TRY_CAST(json_extract_scalar(record, '$.contract_start_date') AS DATE) AS contract_start_date,
                TRY_CAST(json_extract_scalar(record, '$.contract_end_date') AS DATE) AS contract_end_date,
                CAST(json_extract_scalar(record, '$.contract_status') AS VARCHAR) AS contract_status,
                TRY_CAST(json_extract_scalar(record, '$.auto_renew') AS BOOLEAN) AS auto_renew,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_commercial_contracts AS r
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
        WHERE contract_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.contract_id = source.contract_id
WHEN MATCHED THEN
    UPDATE SET
        dealer_id = source.dealer_id,
        contract_start_date = source.contract_start_date,
        contract_end_date = source.contract_end_date,
        contract_status = source.contract_status,
        auto_renew = source.auto_renew,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        contract_id,
        dealer_id,
        contract_start_date,
        contract_end_date,
        contract_status,
        auto_renew,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.contract_id,
        source.dealer_id,
        source.contract_start_date,
        source.contract_end_date,
        source.contract_status,
        source.auto_renew,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
