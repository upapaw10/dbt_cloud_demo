{{ config(
    materialized='table',
    transient=false
) }}

WITH date_gen AS (
    -- Generate about 21 years of dates starting from 2015-01-01
    SELECT 
        DATEADD(day, SEQ4(), TO_DATE('2015-01-01')) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 7670))
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    EXTRACT(day FROM full_date) AS day,
    EXTRACT(month FROM full_date) AS month,
    TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
    EXTRACT(year FROM full_date) AS year,
    EXTRACT(quarter FROM full_date) AS quarter,
    DAYOFWEEK(full_date) AS day_of_week,               -- Sunday = 1 .. Saturday = 7
    TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    WEEKISO(full_date) AS week_of_year,
    DAYOFYEAR(full_date) AS day_of_year,
    CASE WHEN LAST_DAY(full_date) = full_date THEN TRUE ELSE FALSE END AS is_last_day_of_month
FROM date_gen
ORDER BY full_date
