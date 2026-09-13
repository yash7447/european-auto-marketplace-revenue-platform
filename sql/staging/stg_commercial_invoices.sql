MERGE INTO auto_marketplace_staging.stg_commercial_invoices AS target
USING
(
    SELECT
        invoice_id,
        subscription_id,
        dealer_id,
        invoice_date,
        invoice_amount,
        currency,
        payment_status,
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
                PARTITION BY invoice_id
                ORDER BY updated_at DESC NULLS LAST, source_extract_date DESC NULLS LAST, source_run_id DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(json_extract_scalar(record, '$.invoice_id') AS VARCHAR) AS invoice_id,
                CAST(json_extract_scalar(record, '$.subscription_id') AS VARCHAR) AS subscription_id,
                CAST(json_extract_scalar(record, '$.dealer_id') AS VARCHAR) AS dealer_id,
                TRY_CAST(json_extract_scalar(record, '$.invoice_date') AS DATE) AS invoice_date,
                TRY_CAST(json_extract_scalar(record, '$.invoice_amount') AS DECIMAL(14,2)) AS invoice_amount,
                CAST(json_extract_scalar(record, '$.currency') AS VARCHAR) AS currency,
                CAST(json_extract_scalar(record, '$.payment_status') AS VARCHAR) AS payment_status,
                TRY_CAST(json_extract_scalar(record, '$.updated_at') AS DATE) AS updated_at,
                TRY_CAST(extract_date AS DATE) AS source_extract_date,
                CAST(run_id AS VARCHAR) AS source_run_id,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_commercial_invoices AS r
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
        WHERE invoice_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.invoice_id = source.invoice_id
WHEN MATCHED THEN
    UPDATE SET
        subscription_id = source.subscription_id,
        dealer_id = source.dealer_id,
        invoice_date = source.invoice_date,
        invoice_amount = source.invoice_amount,
        currency = source.currency,
        payment_status = source.payment_status,
        updated_at = source.updated_at,
        source_extract_date = source.source_extract_date,
        source_run_id = source.source_run_id,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        invoice_id,
        subscription_id,
        dealer_id,
        invoice_date,
        invoice_amount,
        currency,
        payment_status,
        updated_at,
        source_extract_date,
        source_run_id,
        source_file,
        staged_at
    )
    VALUES
    (
        source.invoice_id,
        source.subscription_id,
        source.dealer_id,
        source.invoice_date,
        source.invoice_amount,
        source.currency,
        source.payment_status,
        source.updated_at,
        source.source_extract_date,
        source.source_run_id,
        source.source_file,
        source.staged_at
    );
