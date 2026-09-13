MERGE INTO auto_marketplace_intermediate.int_listing_engagement t
USING (
    WITH engagement AS (
        SELECT
            listing_id,
            MIN(event_timestamp) AS first_event_at,
            MAX(event_timestamp) AS last_event_at,
            SUM(CASE WHEN LOWER(event_type) = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN LOWER(event_type) = 'click' THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN LOWER(event_type) = 'lead' THEN 1 ELSE 0 END) AS leads,
            COUNT(*) AS engagement_events,
            COUNT(DISTINCT session_id) AS unique_sessions
        FROM auto_marketplace_staging.stg_engagement_events
        WHERE listing_id IS NOT NULL
        GROUP BY listing_id
    )
    SELECT
        l.listing_id,
        l.vehicle_id,
        l.dealer_id,
        l.listing_start_date,
        l.listing_end_date,
        l.asking_price_eur,
        l.listing_status,
        l.visibility_package,
        v.make,
        v.model,
        v.model_year,
        v.fuel_type,
        v.body_type,
        v.mileage_km,
        e.first_event_at,
        e.last_event_at,
        COALESCE(e.views, 0) AS views,
        COALESCE(e.clicks, 0) AS clicks,
        COALESCE(e.leads, 0) AS leads,
        COALESCE(e.engagement_events, 0) AS engagement_events,
        COALESCE(e.unique_sessions, 0) AS unique_sessions,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at
    FROM auto_marketplace_staging.stg_marketplace_listings l
    LEFT JOIN auto_marketplace_staging.stg_marketplace_vehicles v
        ON l.vehicle_id = v.vehicle_id
    LEFT JOIN engagement e
        ON l.listing_id = e.listing_id
) s
ON t.listing_id = s.listing_id
WHEN MATCHED THEN UPDATE SET
    vehicle_id=s.vehicle_id,
    dealer_id=s.dealer_id,
    listing_start_date=s.listing_start_date,
    listing_end_date=s.listing_end_date,
    asking_price_eur=s.asking_price_eur,
    listing_status=s.listing_status,
    visibility_package=s.visibility_package,
    make=s.make,
    model=s.model,
    model_year=s.model_year,
    fuel_type=s.fuel_type,
    body_type=s.body_type,
    mileage_km=s.mileage_km,
    first_event_at=s.first_event_at,
    last_event_at=s.last_event_at,
    views=s.views,
    clicks=s.clicks,
    leads=s.leads,
    engagement_events=s.engagement_events,
    unique_sessions=s.unique_sessions,
    transformed_at=s.transformed_at
WHEN NOT MATCHED THEN INSERT (
    listing_id, vehicle_id, dealer_id, listing_start_date, listing_end_date,
    asking_price_eur, listing_status, visibility_package, make, model,
    model_year, fuel_type, body_type, mileage_km, first_event_at, last_event_at,
    views, clicks, leads, engagement_events, unique_sessions, transformed_at
) VALUES (
    s.listing_id, s.vehicle_id, s.dealer_id, s.listing_start_date,
    s.listing_end_date, s.asking_price_eur, s.listing_status,
    s.visibility_package, s.make, s.model, s.model_year, s.fuel_type,
    s.body_type, s.mileage_km, s.first_event_at, s.last_event_at,
    s.views, s.clicks, s.leads, s.engagement_events, s.unique_sessions,
    s.transformed_at
);
