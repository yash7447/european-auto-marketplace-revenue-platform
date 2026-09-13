MERGE INTO auto_marketplace_analytics.mart_dealer_360 t
USING (
    WITH revenue AS (
        SELECT
            dealer_id,
            CAST(SUM(invoice_amount) AS DECIMAL(18,2)) AS lifetime_revenue_eur,
            COUNT(*) AS invoice_count,
            MAX(invoice_date) AS last_invoice_date
        FROM auto_marketplace_analytics.fact_revenue
        GROUP BY dealer_id
    ),
    subs AS (
        SELECT
            dealer_id,
            SUM(CASE WHEN LOWER(COALESCE(subscription_status, '')) = 'active' THEN 1 ELSE 0 END) AS active_subscriptions,
            COUNT(DISTINCT product_id) AS product_count,
            CAST(AVG(discount_pct) AS DECIMAL(12,4)) AS avg_discount_pct
        FROM auto_marketplace_analytics.fact_subscription
        GROUP BY dealer_id
    ),
    listings AS (
        SELECT
            dealer_id,
            COUNT(*) AS total_listings,
            SUM(CASE WHEN LOWER(COALESCE(listing_status, '')) = 'active' THEN 1 ELSE 0 END) AS active_listings,
            SUM(views) AS views,
            SUM(clicks) AS clicks,
            SUM(leads) AS leads,
            MAX(last_event_at) AS last_engagement_at
        FROM auto_marketplace_analytics.fact_listing_performance
        GROUP BY dealer_id
    )
    SELECT
        d.dealer_id,
        d.dealer_key,
        d.dealer_name,
        d.market,
        d.segment,
        d.sales_rep,
        d.account_status,
        COALESCE(r.lifetime_revenue_eur, DECIMAL '0.00') AS lifetime_revenue_eur,
        COALESCE(r.invoice_count, 0) AS invoice_count,
        COALESCE(s.active_subscriptions, 0) AS active_subscriptions,
        COALESCE(s.product_count, 0) AS product_count,
        COALESCE(s.avg_discount_pct, DECIMAL '0.0000') AS avg_discount_pct,
        COALESCE(l.total_listings, 0) AS total_listings,
        COALESCE(l.active_listings, 0) AS active_listings,
        COALESCE(l.views, 0) AS views,
        COALESCE(l.clicks, 0) AS clicks,
        COALESCE(l.leads, 0) AS leads,
        CAST(
            CASE
                WHEN COALESCE(l.views, 0) = 0 THEN NULL
                ELSE 100.0 * COALESCE(l.leads, 0) / l.views
            END
            AS DECIMAL(12,4)
        ) AS lead_rate_pct,
        r.last_invoice_date,
        l.last_engagement_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS refreshed_at
    FROM auto_marketplace_analytics.dim_dealer d
    LEFT JOIN revenue r ON d.dealer_id=r.dealer_id
    LEFT JOIN subs s ON d.dealer_id=s.dealer_id
    LEFT JOIN listings l ON d.dealer_id=l.dealer_id
) s
ON t.dealer_id=s.dealer_id
WHEN MATCHED THEN UPDATE SET
    dealer_key=s.dealer_key,
    dealer_name=s.dealer_name,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    account_status=s.account_status,
    lifetime_revenue_eur=s.lifetime_revenue_eur,
    invoice_count=s.invoice_count,
    active_subscriptions=s.active_subscriptions,
    product_count=s.product_count,
    avg_discount_pct=s.avg_discount_pct,
    total_listings=s.total_listings,
    active_listings=s.active_listings,
    views=s.views,
    clicks=s.clicks,
    leads=s.leads,
    lead_rate_pct=s.lead_rate_pct,
    last_invoice_date=s.last_invoice_date,
    last_engagement_at=s.last_engagement_at,
    refreshed_at=s.refreshed_at
WHEN NOT MATCHED THEN INSERT (
    dealer_id, dealer_key, dealer_name, market, segment, sales_rep, account_status,
    lifetime_revenue_eur, invoice_count, active_subscriptions, product_count,
    avg_discount_pct, total_listings, active_listings, views, clicks, leads,
    lead_rate_pct, last_invoice_date, last_engagement_at, refreshed_at
) VALUES (
    s.dealer_id, s.dealer_key, s.dealer_name, s.market, s.segment, s.sales_rep,
    s.account_status, s.lifetime_revenue_eur, s.invoice_count,
    s.active_subscriptions, s.product_count, s.avg_discount_pct,
    s.total_listings, s.active_listings, s.views, s.clicks, s.leads,
    s.lead_rate_pct, s.last_invoice_date, s.last_engagement_at, s.refreshed_at
);
