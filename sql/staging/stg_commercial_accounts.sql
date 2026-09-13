MERGE INTO auto_marketplace_staging.stg_commercial_accounts AS target
USING
(
    SELECT
        dealer_id,
        dealer_name,
        market,
        segment,
        sales_rep,
        account_status,
        created_date,
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
                PARTITION BY dealer_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.dealer_id') AS VARCHAR) AS dealer_id,
                CAST(json_extract_scalar(record, '$.dealer_name') AS VARCHAR) AS dealer_name,
                CAST(json_extract_scalar(record, '$.market') AS VARCHAR) AS market,
                CAST(json_extract_scalar(record, '$.segment') AS VARCHAR) AS segment,
                CAST(json_extract_scalar(record, '$.sales_rep') AS VARCHAR) AS sales_rep,
                CAST(json_extract_scalar(record, '$.account_status') AS VARCHAR) AS account_status,
                TRY_CAST(json_extract_scalar(record, '$.created_date') AS DATE) AS created_date,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_commercial_accounts AS r
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
        WHERE dealer_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.dealer_id = source.dealer_id
WHEN MATCHED THEN
    UPDATE SET
        dealer_name = source.dealer_name,
        market = source.market,
        segment = source.segment,
        sales_rep = source.sales_rep,
        account_status = source.account_status,
        created_date = source.created_date,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        dealer_id,
        dealer_name,
        market,
        segment,
        sales_rep,
        account_status,
        created_date,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.dealer_id,
        source.dealer_name,
        source.market,
        source.segment,
        source.sales_rep,
        source.account_status,
        source.created_date,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
