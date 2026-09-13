MERGE INTO auto_marketplace_analytics.mart_sales_performance_monthly t
USING (
    WITH revenue AS (
        SELECT
            CAST(date_trunc('month', invoice_date) AS DATE) AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            COALESCE(sales_rep, 'UNKNOWN') AS sales_rep,
            COALESCE(product_family, 'UNKNOWN') AS product_group,
            CAST(SUM(invoice_amount) AS DECIMAL(18,2)) AS revenue_eur,
            COUNT(*) AS invoice_count,
            COUNT(DISTINCT dealer_id) AS revenue_dealer_count
        FROM auto_marketplace_analytics.fact_revenue
        WHERE invoice_date IS NOT NULL
        GROUP BY 1,2,3,4
    ),

    targets AS (
        SELECT
            target_month AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            COALESCE(sales_rep, 'UNKNOWN') AS sales_rep,
            COALESCE(product_group, 'UNKNOWN') AS product_group,
            CAST(SUM(revenue_target_eur) AS DECIMAL(18,2)) AS revenue_target_eur,
            CAST(SUM(upsell_target_eur) AS DECIMAL(18,2)) AS upsell_target_eur,
            SUM(CAST(new_dealer_target AS BIGINT)) AS new_dealer_target,
            CAST(AVG(retention_target_pct) AS DECIMAL(7,4)) AS retention_target_pct
        FROM auto_marketplace_analytics.fact_sales_target
        WHERE target_month IS NOT NULL
        GROUP BY 1,2,3,4
    ),

    subs AS (
        SELECT
            CAST(date_trunc('month', subscription_start_date) AS DATE) AS month_start,
            COALESCE(market, 'UNKNOWN') AS market,
            COALESCE(sales_rep, 'UNKNOWN') AS sales_rep,
            COALESCE(product_family, 'UNKNOWN') AS product_group,
            COUNT(*) AS active_subscriptions
        FROM auto_marketplace_analytics.fact_subscription
        WHERE
            subscription_start_date IS NOT NULL
            AND LOWER(COALESCE(subscription_status, '')) = 'active'
        GROUP BY 1,2,3,4
    ),

    keys AS (
        SELECT month_start, market, sales_rep, product_group FROM revenue
        UNION
        SELECT month_start, market, sales_rep, product_group FROM targets
        UNION
        SELECT month_start, market, sales_rep, product_group FROM subs
    )

    SELECT
        to_hex(
            md5(
                to_utf8(
                    concat(
                        CAST(k.month_start AS VARCHAR), '|',
                        k.market, '|',
                        k.sales_rep, '|',
                        k.product_group
                    )
                )
            )
        ) AS performance_key,

        k.month_start,
        k.market,
        k.sales_rep,
        k.product_group,

        COALESCE(
            r.revenue_eur,
            DECIMAL '0.00'
        ) AS revenue_eur,

        COALESCE(
            tg.revenue_target_eur,
            DECIMAL '0.00'
        ) AS revenue_target_eur,

        COALESCE(
            tg.upsell_target_eur,
            DECIMAL '0.00'
        ) AS upsell_target_eur,

        COALESCE(
            tg.new_dealer_target,
            0
        ) AS new_dealer_target,

        COALESCE(
            tg.retention_target_pct,
            DECIMAL '0.0000'
        ) AS retention_target_pct,

        CAST(
            CASE
                WHEN COALESCE(
                    tg.revenue_target_eur,
                    DECIMAL '0.00'
                ) = DECIMAL '0.00'
                    THEN NULL

                ELSE
                    100.0
                    * COALESCE(
                        r.revenue_eur,
                        DECIMAL '0.00'
                    )
                    / tg.revenue_target_eur
            END
            AS DECIMAL(12,4)
        ) AS attainment_pct,

        COALESCE(
            r.invoice_count,
            0
        ) AS invoice_count,

        COALESCE(
            r.revenue_dealer_count,
            0
        ) AS revenue_dealer_count,

        COALESCE(
            s.active_subscriptions,
            0
        ) AS active_subscriptions,

        CAST(
            CURRENT_TIMESTAMP
            AS TIMESTAMP
        ) AS refreshed_at

    FROM keys k

    LEFT JOIN revenue r
        ON k.month_start = r.month_start
       AND k.market = r.market
       AND k.sales_rep = r.sales_rep
       AND k.product_group = r.product_group

    LEFT JOIN targets tg
        ON k.month_start = tg.month_start
       AND k.market = tg.market
       AND k.sales_rep = tg.sales_rep
       AND k.product_group = tg.product_group

    LEFT JOIN subs s
        ON k.month_start = s.month_start
       AND k.market = s.market
       AND k.sales_rep = s.sales_rep
       AND k.product_group = s.product_group

    WHERE k.month_start IS NOT NULL
) s

ON t.performance_key = s.performance_key

WHEN MATCHED THEN
    UPDATE SET
        month_start = s.month_start,
        market = s.market,
        sales_rep = s.sales_rep,
        product_group = s.product_group,
        revenue_eur = s.revenue_eur,
        revenue_target_eur = s.revenue_target_eur,
        upsell_target_eur = s.upsell_target_eur,
        new_dealer_target = s.new_dealer_target,
        retention_target_pct = s.retention_target_pct,
        attainment_pct = s.attainment_pct,
        invoice_count = s.invoice_count,
        revenue_dealer_count = s.revenue_dealer_count,
        active_subscriptions = s.active_subscriptions,
        refreshed_at = s.refreshed_at

WHEN NOT MATCHED THEN
    INSERT (
        performance_key,
        month_start,
        market,
        sales_rep,
        product_group,
        revenue_eur,
        revenue_target_eur,
        upsell_target_eur,
        new_dealer_target,
        retention_target_pct,
        attainment_pct,
        invoice_count,
        revenue_dealer_count,
        active_subscriptions,
        refreshed_at
    )
    VALUES (
        s.performance_key,
        s.month_start,
        s.market,
        s.sales_rep,
        s.product_group,
        s.revenue_eur,
        s.revenue_target_eur,
        s.upsell_target_eur,
        s.new_dealer_target,
        s.retention_target_pct,
        s.attainment_pct,
        s.invoice_count,
        s.revenue_dealer_count,
        s.active_subscriptions,
        s.refreshed_at
    );
