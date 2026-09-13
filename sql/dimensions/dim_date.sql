MERGE INTO auto_marketplace_analytics.dim_date t
USING (
    SELECT
        CAST(
            year(d) * 10000
            + month(d) * 100
            + day(d)
            AS INTEGER
        ) AS date_key,
        d AS calendar_date,
        CAST(year(d) AS INTEGER) AS year_number,
        CAST(quarter(d) AS INTEGER) AS quarter_number,
        CAST(month(d) AS INTEGER) AS month_number,
        date_format(d, '%M') AS month_name,
        date_format(d, '%Y-%m') AS year_month,
        CAST(week(d) AS INTEGER) AS week_of_year,
        CAST(day(d) AS INTEGER) AS day_of_month,
        CAST(day_of_week(d) AS INTEGER) AS day_of_week,
        date_format(d, '%W') AS day_name,
        CASE WHEN day_of_week(d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM UNNEST(
        SEQUENCE(
            DATE '2024-01-01',
            DATE '2028-12-31',
            INTERVAL '1' DAY
        )
    ) AS x(d)
) s
ON t.date_key = s.date_key
WHEN MATCHED THEN UPDATE SET
    calendar_date=s.calendar_date,
    year_number=s.year_number,
    quarter_number=s.quarter_number,
    month_number=s.month_number,
    month_name=s.month_name,
    year_month=s.year_month,
    week_of_year=s.week_of_year,
    day_of_month=s.day_of_month,
    day_of_week=s.day_of_week,
    day_name=s.day_name,
    is_weekend=s.is_weekend,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    date_key, calendar_date, year_number, quarter_number, month_number,
    month_name, year_month, week_of_year, day_of_month, day_of_week,
    day_name, is_weekend, loaded_at
) VALUES (
    s.date_key, s.calendar_date, s.year_number, s.quarter_number, s.month_number,
    s.month_name, s.year_month, s.week_of_year, s.day_of_month, s.day_of_week,
    s.day_name, s.is_weekend, s.loaded_at
);
