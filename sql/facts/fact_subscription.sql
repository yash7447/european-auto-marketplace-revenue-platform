MERGE INTO auto_marketplace_analytics.fact_subscription t
USING (
    SELECT
        s.subscription_id,
        dd.dealer_key,
        dp.product_key,
        s.dealer_id,
        s.product_id,
        s.contract_id,
        s.market,
        s.segment,
        s.sales_rep,
        s.product_family,
        s.contract_price,
        s.discount_pct,
        s.billing_frequency,
        s.subscription_status,
        s.contract_status,
        s.auto_renew,
        s.subscription_start_date,
        s.subscription_end_date,
        ds.date_key AS start_date_key,
        de.date_key AS end_date_key,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_intermediate.int_dealer_subscription_snapshot s
    LEFT JOIN auto_marketplace_analytics.dim_dealer dd
        ON s.dealer_id = dd.dealer_id
    LEFT JOIN auto_marketplace_analytics.dim_product dp
        ON s.product_id = dp.product_id
    LEFT JOIN auto_marketplace_analytics.dim_date ds
        ON s.subscription_start_date = ds.calendar_date
    LEFT JOIN auto_marketplace_analytics.dim_date de
        ON s.subscription_end_date = de.calendar_date
) s
ON t.subscription_id = s.subscription_id
WHEN MATCHED THEN UPDATE SET
    dealer_key=s.dealer_key,
    product_key=s.product_key,
    dealer_id=s.dealer_id,
    product_id=s.product_id,
    contract_id=s.contract_id,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    product_family=s.product_family,
    contract_price=s.contract_price,
    discount_pct=s.discount_pct,
    billing_frequency=s.billing_frequency,
    subscription_status=s.subscription_status,
    contract_status=s.contract_status,
    auto_renew=s.auto_renew,
    subscription_start_date=s.subscription_start_date,
    subscription_end_date=s.subscription_end_date,
    start_date_key=s.start_date_key,
    end_date_key=s.end_date_key,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    subscription_id, dealer_key, product_key, dealer_id, product_id, contract_id,
    market, segment, sales_rep, product_family, contract_price, discount_pct,
    billing_frequency, subscription_status, contract_status, auto_renew,
    subscription_start_date, subscription_end_date, start_date_key, end_date_key,
    loaded_at
) VALUES (
    s.subscription_id, s.dealer_key, s.product_key, s.dealer_id, s.product_id,
    s.contract_id, s.market, s.segment, s.sales_rep, s.product_family,
    s.contract_price, s.discount_pct, s.billing_frequency, s.subscription_status,
    s.contract_status, s.auto_renew, s.subscription_start_date,
    s.subscription_end_date, s.start_date_key, s.end_date_key, s.loaded_at
);
