SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_engagement_events e
LEFT JOIN auto_marketplace_staging.stg_marketplace_listings l
    ON e.listing_id = l.listing_id
WHERE
    e.listing_id IS NOT NULL
    AND l.listing_id IS NULL;
