MERGE INTO auto_marketplace_analytics.fact_revenue t
USING (
    SELECT
        i.invoice_id,
        i.invoice_date,
        d.date_key,
        dd.dealer_key,
        dp.product_key,
        i.dealer_id,
        i.product_id,
        i.subscription_id,
        i.contract_id,
        i.market,
        i.segment,
        i.sales_rep,
        i.product_family,
        i.invoice_amount,
        i.currency,
        i.payment_status,
        i.contract_price,
        i.discount_pct,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_intermediate.int_invoice_enriched i
    LEFT JOIN auto_marketplace_analytics.dim_date d
        ON i.invoice_date = d.calendar_date
    LEFT JOIN auto_marketplace_analytics.dim_dealer dd
        ON i.dealer_id = dd.dealer_id
    LEFT JOIN auto_marketplace_analytics.dim_product dp
        ON i.product_id = dp.product_id
) s
ON t.invoice_id = s.invoice_id
WHEN MATCHED THEN UPDATE SET
    invoice_date=s.invoice_date,
    date_key=s.date_key,
    dealer_key=s.dealer_key,
    product_key=s.product_key,
    dealer_id=s.dealer_id,
    product_id=s.product_id,
    subscription_id=s.subscription_id,
    contract_id=s.contract_id,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    product_family=s.product_family,
    invoice_amount=s.invoice_amount,
    currency=s.currency,
    payment_status=s.payment_status,
    contract_price=s.contract_price,
    discount_pct=s.discount_pct,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    invoice_id, invoice_date, date_key, dealer_key, product_key, dealer_id,
    product_id, subscription_id, contract_id, market, segment, sales_rep,
    product_family, invoice_amount, currency, payment_status, contract_price,
    discount_pct, loaded_at
) VALUES (
    s.invoice_id, s.invoice_date, s.date_key, s.dealer_key, s.product_key,
    s.dealer_id, s.product_id, s.subscription_id, s.contract_id, s.market,
    s.segment, s.sales_rep, s.product_family, s.invoice_amount, s.currency,
    s.payment_status, s.contract_price, s.discount_pct, s.loaded_at
);
