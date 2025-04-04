{{
    config(
        tags = ["intermediate","grr_nrr"]
    )
}}
WITH month_revenue AS(
    SELECT DISTINCT
        product_id
        , payment_month
        , SUM(revenue) AS total_revenue
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        product_id
        , payment_month
),
cal_prev_revenue AS(
    SELECT
        product_id
        , payment_month
        , total_revenue
    FROM
        month_revenue
),
removing_null AS(
    SELECT
        product_id
        , payment_month
        , total_revenue
    FROM
        cal_prev_revenue
),
churn_customers AS(
    SELECT
        c.product_id AS product_id
        , c.payment_month AS month_year
        , c.total_revenue AS total_revenue
        , t.revenue AS revenue
        , CAST(MIN(t.payment_month) AS DATE) AS min_payment_month
        , CAST(MAX(t.payment_month) AS DATE) AS max_payment_month
    FROM
        {{ ref('stg_transactions') }} AS t
        INNER JOIN
            removing_null AS c
            ON
                t.product_id = c.product_id
    GROUP BY
        c.product_id
        , c.payment_month
        , c.total_revenue
        , t.revenue
),
cal_churn_month AS(
    SELECT
        product_id
        , month_year
        , total_revenue
        , SUM(revenue) AS churn_revenue
    FROM
        churn_customers
    WHERE
        EXTRACT(YEAR FROM max_payment_month) = EXTRACT(YEAR FROM month_year) AND
        EXTRACT(MONTH FROM max_payment_month) = EXTRACT(MONTH FROM month_year)-1
    GROUP BY
        product_id
        , month_year
        , total_revenue   
),
cal_grr AS(
    SELECT
        product_id
        , month_year
        , total_revenue
        , churn_revenue
        , ((total_revenue-churn_revenue)/total_revenue)*100 AS grr
    FROM
        cal_churn_month
),
cal_nrr AS(
    SELECT
        c.product_id
        , c.month_year
        , c.total_revenue
        , c.churn_revenue
        , c.grr
        , r.crossell_count AS crossell_value
        , ((c.total_revenue-c.churn_revenue+r.crossell_count)/total_revenue)*100 AS nrr
    FROM
        cal_grr c
        LEFT JOIN
            {{ ref('int_crossell_nrr') }} r
        ON
            c.product_id = r.product_id
            AND c.month_year = r.payment_month
)
SELECT
    *
FROM
    cal_nrr
ORDER BY
    product_id
    , month_year

-- SELECT
--     *
-- FROM
--     product_churn
