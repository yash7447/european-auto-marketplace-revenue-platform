MERGE INTO auto_marketplace_analytics.fact_listing_performance t
USING (
    SELECT
        l.listing_id,
        dd.dealer_key,
        dv.vehicle_key,
        l.dealer_id,
        l.vehicle_id,
        l.listing_start_date,
        l.listing_end_date,
        ds.date_key AS start_date_key,
        de.date_key AS end_date_key,
        l.asking_price_eur,
        l.listing_status,
        l.visibility_package,
        l.views,
        l.clicks,
        l.leads,
        l.engagement_events,
        l.unique_sessions,
        l.first_event_at,
        l.last_event_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_intermediate.int_listing_engagement l
    LEFT JOIN auto_marketplace_analytics.dim_dealer dd
        ON l.dealer_id = dd.dealer_id
    LEFT JOIN auto_marketplace_analytics.dim_vehicle dv
        ON l.vehicle_id = dv.vehicle_id
    LEFT JOIN auto_marketplace_analytics.dim_date ds
        ON l.listing_start_date = ds.calendar_date
    LEFT JOIN auto_marketplace_analytics.dim_date de
        ON l.listing_end_date = de.calendar_date
) s
ON t.listing_id = s.listing_id
WHEN MATCHED THEN UPDATE SET
    dealer_key=s.dealer_key,
    vehicle_key=s.vehicle_key,
    dealer_id=s.dealer_id,
    vehicle_id=s.vehicle_id,
    listing_start_date=s.listing_start_date,
    listing_end_date=s.listing_end_date,
    start_date_key=s.start_date_key,
    end_date_key=s.end_date_key,
    asking_price_eur=s.asking_price_eur,
    listing_status=s.listing_status,
    visibility_package=s.visibility_package,
    views=s.views,
    clicks=s.clicks,
    leads=s.leads,
    engagement_events=s.engagement_events,
    unique_sessions=s.unique_sessions,
    first_event_at=s.first_event_at,
    last_event_at=s.last_event_at,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    listing_id, dealer_key, vehicle_key, dealer_id, vehicle_id,
    listing_start_date, listing_end_date, start_date_key, end_date_key,
    asking_price_eur, listing_status, visibility_package, views, clicks, leads,
    engagement_events, unique_sessions, first_event_at, last_event_at, loaded_at
) VALUES (
    s.listing_id, s.dealer_key, s.vehicle_key, s.dealer_id, s.vehicle_id,
    s.listing_start_date, s.listing_end_date, s.start_date_key, s.end_date_key,
    s.asking_price_eur, s.listing_status, s.visibility_package, s.views,
    s.clicks, s.leads, s.engagement_events, s.unique_sessions,
    s.first_event_at, s.last_event_at, s.loaded_at
);
