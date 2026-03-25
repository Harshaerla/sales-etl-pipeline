{{ config(
    materialized = 'view',
    schema = 'gold'
) }}

SELECT DISTINCT
    TO_NUMBER(TO_CHAR(sls_order_dt, 'YYYYMMDD'))    AS date_key,
    sls_order_dt                                     AS full_date,
    DAY(sls_order_dt)                               AS day,
    MONTH(sls_order_dt)                             AS month,
    MONTHNAME(sls_order_dt)                         AS month_name,
    QUARTER(sls_order_dt)                           AS quarter,
    YEAR(sls_order_dt)                              AS year,
    DAYNAME(sls_order_dt)                           AS day_name,
    CASE
        WHEN DAYOFWEEK(sls_order_dt) IN (1, 7)
        THEN TRUE ELSE FALSE
    END                                             AS is_weekend,
    CASE
        WHEN MONTH(sls_order_dt) <= 6
        THEN 'H1' ELSE 'H2'
    END                                             AS half_year

FROM {{ ref('erp_sales_details') }}
WHERE sls_order_dt IS NOT NULL