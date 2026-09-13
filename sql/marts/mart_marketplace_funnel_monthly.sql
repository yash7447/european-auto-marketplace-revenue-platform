MERGE INTO auto_marketplace_analytics.mart_marketplace_funnel_monthly t
USING (
    SELECT
        to_hex(
            md5(
                to_utf8(
                    concat(
                        CAST(CAST(date_trunc('month', e.event_date) AS DATE) AS VARCHAR),
                        '|',
                        COALESCE(a.market, 'UNKNOWN')
                    )
                )
            )
        ) AS funnel_key,
        CAST(date_trunc('month', e.event_date) AS DATE) AS month_start,
        COALESCE(a.market, 'UNKNOWN') AS market,
        SUM(CASE WHEN LOWER(e.event_type)='view' THEN 1 ELSE 0 END) AS views,
        SUM(CASE WHEN LOWER(e.event_type)='click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN LOWER(e.event_type)='lead' THEN 1 ELSE 0 END) AS leads,
        COUNT(DISTINCT e.session_id) AS unique_sessions,
        COUNT(DISTINCT e.listing_id) AS listings_with_activity,
        COUNT(DISTINCT e.dealer_id) AS dealers_with_activity,
        CAST(
            CASE
                WHEN SUM(CASE WHEN LOWER(e.event_type)='view' THEN 1 ELSE 0 END)=0
                    THEN NULL
                ELSE 100.0 * SUM(CASE WHEN LOWER(e.event_type)='click' THEN 1 ELSE 0 END)
                    / SUM(CASE WHEN LOWER(e.event_type)='view' THEN 1 ELSE 0 END)
            END
            AS DECIMAL(12,4)
        ) AS click_rate_pct,
        CAST(
            CASE
                WHEN SUM(CASE WHEN LOWER(e.event_type)='view' THEN 1 ELSE 0 END)=0
                    THEN NULL
                ELSE 100.0 * SUM(CASE WHEN LOWER(e.event_type)='lead' THEN 1 ELSE 0 END)
                    / SUM(CASE WHEN LOWER(e.event_type)='view' THEN 1 ELSE 0 END)
            END
            AS DECIMAL(12,4)
        ) AS lead_rate_pct,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS refreshed_at
    FROM auto_marketplace_staging.stg_engagement_events e
    LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
        ON e.dealer_id=a.dealer_id
    GROUP BY 2,3
) s
ON t.funnel_key=s.funnel_key
WHEN MATCHED THEN UPDATE SET
    month_start=s.month_start,
    market=s.market,
    views=s.views,
    clicks=s.clicks,
    leads=s.leads,
    unique_sessions=s.unique_sessions,
    listings_with_activity=s.listings_with_activity,
    dealers_with_activity=s.dealers_with_activity,
    click_rate_pct=s.click_rate_pct,
    lead_rate_pct=s.lead_rate_pct,
    refreshed_at=s.refreshed_at
WHEN NOT MATCHED THEN INSERT (
    funnel_key, month_start, market, views, clicks, leads, unique_sessions,
    listings_with_activity, dealers_with_activity, click_rate_pct,
    lead_rate_pct, refreshed_at
) VALUES (
    s.funnel_key, s.month_start, s.market, s.views, s.clicks, s.leads,
    s.unique_sessions, s.listings_with_activity, s.dealers_with_activity,
    s.click_rate_pct, s.lead_rate_pct, s.refreshed_at
);
