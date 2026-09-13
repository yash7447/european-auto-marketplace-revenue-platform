MERGE INTO auto_marketplace_analytics.fact_sales_target t
USING (
    SELECT
        st.target_id,
        st.target_month,
        d.date_key,
        st.market,
        st.sales_rep,
        st.product_group,
        st.revenue_target_eur,
        st.upsell_target_eur,
        st.new_dealer_target,
        st.retention_target_pct,
        st.planning_version,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_intermediate.int_sales_targets st
    LEFT JOIN auto_marketplace_analytics.dim_date d
        ON st.target_month = d.calendar_date
) s
ON t.target_id = s.target_id
WHEN MATCHED THEN UPDATE SET
    target_month=s.target_month,
    date_key=s.date_key,
    market=s.market,
    sales_rep=s.sales_rep,
    product_group=s.product_group,
    revenue_target_eur=s.revenue_target_eur,
    upsell_target_eur=s.upsell_target_eur,
    new_dealer_target=s.new_dealer_target,
    retention_target_pct=s.retention_target_pct,
    planning_version=s.planning_version,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    target_id, target_month, date_key, market, sales_rep, product_group,
    revenue_target_eur, upsell_target_eur, new_dealer_target,
    retention_target_pct, planning_version, loaded_at
) VALUES (
    s.target_id, s.target_month, s.date_key, s.market, s.sales_rep,
    s.product_group, s.revenue_target_eur, s.upsell_target_eur,
    s.new_dealer_target, s.retention_target_pct, s.planning_version, s.loaded_at
);
