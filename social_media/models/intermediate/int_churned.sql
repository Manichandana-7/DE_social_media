{{
    config(
        tags = ["intermediate","churned"]
    )
}}

WITH adjusted_date AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , ROW_NUMBER() OVER (PARTITION BY customer_id, product_id ORDER BY payment_month ASC) AS row_num
        , LAG(payment_month) OVER (PARTITION BY customer_id, product_id ORDER BY payment_month ASC) AS prev_payment_month
        , MIN(payment_month) OVER (PARTITION BY customer_id, product_id) AS min_payment_month
        , MAX(payment_month) OVER (PARTITION BY customer_id, product_id) AS max_payment_month
        , {{get_dynamic_date('payment_month')}} AS adjusted_month
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        customer_id
        , product_id
        , payment_month
),
removing_null_pre_payment AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , row_num
        , CASE
            WHEN prev_payment_month IS NULL THEN payment_month
            ELSE prev_payment_month
          END AS pre_payment_month
        , DATEDIFF('month', prev_payment_month, payment_month) AS diff_month
        , min_payment_month
        , max_payment_month
        , adjusted_month
    FROM
        adjusted_date  
        
),
removing_null_diff_month AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , row_num
        , pre_payment_month
        , CASE
            WHEN diff_month IS NULL THEN 0
            ELSE diff_month
          END AS diff_month
        , min_payment_month
        , max_payment_month
        , adjusted_month
    FROM
        removing_null_pre_payment
),
churned_or_new AS(
    SELECT
        customer_id
        , product_id
        , payment_month
        , row_num
        , pre_payment_month
        , diff_month
        , min_payment_month
        , max_payment_month
        , adjusted_month
        , CASE
            WHEN diff_month = 0 THEN 'New customer'
            WHEN diff_month = 1 THEN 'Active customer'
            ELSE 'New customer'
          END AS churned_or_new
    FROM
        removing_null_diff_month
)
SELECT 
    *
FROM
    churned_or_new
ORDER BY
    customer_id 
    , payment_month
    , row_num ASC