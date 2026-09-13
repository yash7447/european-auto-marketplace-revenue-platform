MERGE INTO auto_marketplace_analytics.mart_product_performance_monthly t
USING (
    WITH revenue AS (
        SELECT
            CAST(date_trunc('month', invoice_date) AS DATE) AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            product_id,
            MAX(product_family) AS product_family,
            CAST(SUM(invoice_amount) AS DECIMAL(18,2)) AS revenue_eur,
            COUNT(*) AS invoice_count,
            COUNT(DISTINCT dealer_id) AS dealer_count
        FROM auto_marketplace_analytics.fact_revenue
        WHERE product_id IS NOT NULL
        GROUP BY 1,2,3
    ),
    subs AS (
        SELECT
            CAST(date_trunc('month', subscription_start_date) AS DATE) AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            product_id,
            SUM(CASE WHEN LOWER(COALESCE(subscription_status, ''))='active' THEN 1 ELSE 0 END) AS active_subscriptions,
            CAST(AVG(contract_price) AS DECIMAL(14,2)) AS avg_contract_price,
            CAST(AVG(discount_pct) AS DECIMAL(12,4)) AS avg_discount_pct
        FROM auto_marketplace_analytics.fact_subscription
        WHERE product_id IS NOT NULL
        GROUP BY 1,2,3
    ),
    prices AS (
        SELECT
            CAST(date_trunc('month', effective_from) AS DATE) AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            product_id,
            CAST(AVG(list_price) AS DECIMAL(14,2)) AS avg_list_price
        FROM auto_marketplace_intermediate.int_price_book
        WHERE product_id IS NOT NULL
        GROUP BY 1,2,3
    ),
    keys AS (
        SELECT month_start, market, product_id FROM revenue
        UNION
        SELECT month_start, market, product_id FROM subs
        UNION
        SELECT month_start, market, product_id FROM prices
    )
    SELECT
        to_hex(
            md5(
                to_utf8(
                    concat(
                        CAST(k.month_start AS VARCHAR), '|',
                        k.market, '|', k.product_id
                    )
                )
            )
        ) AS product_month_key,
        k.month_start,
        k.market,
        k.product_id,
        dp.product_name,
        dp.product_family,
        COALESCE(r.revenue_eur, DECIMAL '0.00') AS revenue_eur,
        COALESCE(r.invoice_count, 0) AS invoice_count,
        COALESCE(r.dealer_count, 0) AS dealer_count,
        COALESCE(s.active_subscriptions, 0) AS active_subscriptions,
        s.avg_contract_price,
        s.avg_discount_pct,
        p.avg_list_price,
        CAST(
            CASE
                WHEN COALESCE(r.dealer_count, 0) = 0 THEN NULL
                ELSE COALESCE(r.revenue_eur, DECIMAL '0.00') / r.dealer_count
            END
            AS DECIMAL(18,2)
        ) AS revenue_per_dealer,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS refreshed_at
    FROM keys k
    LEFT JOIN revenue r
        ON k.month_start=r.month_start AND k.market=r.market AND k.product_id=r.product_id
    LEFT JOIN subs s
        ON k.month_start=s.month_start AND k.market=s.market AND k.product_id=s.product_id
    LEFT JOIN prices p
        ON k.month_start=p.month_start AND k.market=p.market AND k.product_id=p.product_id
    LEFT JOIN auto_marketplace_analytics.dim_product dp
        ON k.product_id=dp.product_id
) s
ON t.product_month_key=s.product_month_key
WHEN MATCHED THEN UPDATE SET
    month_start=s.month_start,
    market=s.market,
    product_id=s.product_id,
    product_name=s.product_name,
    product_family=s.product_family,
    revenue_eur=s.revenue_eur,
    invoice_count=s.invoice_count,
    dealer_count=s.dealer_count,
    active_subscriptions=s.active_subscriptions,
    avg_contract_price=s.avg_contract_price,
    avg_discount_pct=s.avg_discount_pct,
    avg_list_price=s.avg_list_price,
    revenue_per_dealer=s.revenue_per_dealer,
    refreshed_at=s.refreshed_at
WHEN NOT MATCHED THEN INSERT (
    product_month_key, month_start, market, product_id, product_name,
    product_family, revenue_eur, invoice_count, dealer_count,
    active_subscriptions, avg_contract_price, avg_discount_pct,
    avg_list_price, revenue_per_dealer, refreshed_at
) VALUES (
    s.product_month_key, s.month_start, s.market, s.product_id, s.product_name,
    s.product_family, s.revenue_eur, s.invoice_count, s.dealer_count,
    s.active_subscriptions, s.avg_contract_price, s.avg_discount_pct,
    s.avg_list_price, s.revenue_per_dealer, s.refreshed_at
);
