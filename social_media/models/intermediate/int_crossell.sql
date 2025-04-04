{{
    config(
        tags = ["intermediate","crossell"]
    )
}}
WITH customer_product AS(
    SELECT
        customer_id
        , product_id
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        customer_id
        , product_id
),
joining AS(
    SELECT
        distinct
        c.*
        , MIN(payment_month) OVER(PARTITION BY c.customer_id,c.product_id)  AS min_payment_month
        , MAX(payment_month) OVER(PARTITION BY c.customer_id,c.product_id)  AS max_payment_month
        , MIN(payment_month) OVER(PARTITION BY c.customer_id)               AS customer_min_payment_month
        , MAX(payment_month) OVER(PARTITION BY c.customer_id)               AS customer_max_payment_month
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
        , min_payment_month
        , max_payment_month
        , customer_min_payment_month
        , customer_max_payment_month
        , CASE 
            WHEN min_payment_month > customer_min_payment_month THEN 1
            ELSE 0
          END AS crossell
        , CASE 
            WHEN max_payment_month < customer_max_payment_month THEN 1
            ELSE 0
          END AS product_churn
    FROM
        joining
)
SELECT
    customer_id
    , SUM(crossell) AS crossell_count
    , SUM(product_churn) AS product_churn_count
FROM
    counting_crossell
GROUP BY
    customer_id
ORDER BY
    customer_id


  
