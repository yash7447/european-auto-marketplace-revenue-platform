MERGE INTO auto_marketplace_staging.stg_engagement_events AS target
USING
(
    SELECT
        event_id,
        event_timestamp,
        event_date,
        listing_id,
        dealer_id,
        session_id,
        event_type,
        device,
        source_file,
        staged_at
    FROM
    (
        SELECT
            normalized.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY event_id
                ORDER BY event_timestamp DESC NULLS LAST,
                         event_date DESC NULLS LAST,
                         source_file DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(event_id AS VARCHAR) AS event_id,
                TRY_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
                TRY_CAST(event_date AS DATE) AS event_date,
                CAST(listing_id AS VARCHAR) AS listing_id,
                CAST(dealer_id AS VARCHAR) AS dealer_id,
                CAST(session_id AS VARCHAR) AS session_id,
                CAST(event_type AS VARCHAR) AS event_type,
                CAST(device AS VARCHAR) AS device,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_engagement_events
            WHERE
                __SOURCE_FILTER__
                AND "$path" LIKE '%.jsonl.gz'
        ) AS normalized
        WHERE event_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
    UPDATE SET
        event_timestamp = source.event_timestamp,
        event_date = source.event_date,
        listing_id = source.listing_id,
        dealer_id = source.dealer_id,
        session_id = source.session_id,
        event_type = source.event_type,
        device = source.device,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
        event_id,
        event_timestamp,
        event_date,
        listing_id,
        dealer_id,
        session_id,
        event_type,
        device,
        source_file,
        staged_at
    )
    VALUES
    (
        source.event_id,
        source.event_timestamp,
        source.event_date,
        source.listing_id,
        source.dealer_id,
        source.session_id,
        source.event_type,
        source.device,
        source.source_file,
        source.staged_at
    );
