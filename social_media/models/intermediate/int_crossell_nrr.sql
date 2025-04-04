{{
    config(
        tags = ["intermediate","crossell"]
    )
}}
WITH customer_product AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , revenue
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        customer_id
        , product_id
        , revenue
        , payment_month
),
joining AS(
    SELECT
        distinct
        c.*
        , MIN(c.payment_month) OVER(PARTITION BY c.customer_id,c.product_id)  AS min_payment_month
        , MIN(c.payment_month) OVER(PARTITION BY c.customer_id)               AS customer_min_payment_month
    FROM
        customer_product c
        INNER JOIN
            {{ ref('stg_transactions') }} t
        ON
            c.customer_id = t.customer_id
            AND c.product_id = t.product_id
),
counting_crossell AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , min_payment_month
        , customer_min_payment_month
        , CASE 
            WHEN min_payment_month > customer_min_payment_month THEN revenue
            ELSE 0
          END AS crossell
    FROM
        joining
)
-- cal_crossell AS(
    SELECT
        product_id
        , payment_month
        , SUM(crossell) AS crossell_count
    FROM
        counting_crossell
    GROUP BY
        product_id
        , payment_month
    ORDER BY
        product_id
-- )

  
