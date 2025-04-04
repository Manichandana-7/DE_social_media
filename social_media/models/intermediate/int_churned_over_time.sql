{{
    config(
        tags = ["intermediate"]
    )
}}
WITH max_and_min_customer AS (
    SELECT
        customer_id
        , MIN(payment_month) AS min_payment_month
        , MAX(payment_month) AS max_payment_month
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        customer_id
),
min_and_max AS (
    SELECT
        MIN(payment_month) AS min_payment
        , MAX(payment_month) AS max_payment
    FROM
        {{ ref('stg_transactions') }}
),
churned_or_new AS (
    SELECT
        customer_id
        , min_payment_month
        , max_payment_month
        , CASE
            WHEN max_payment_month <= DATEADD({{var('period_type')}},-{{var('period_range')}},(SELECT max_payment FROM min_and_max)) THEN 'churned'
            WHEN min_payment_month >= DATEADD({{var('period_type')}},-{{var('period_range')}},(SELECT min_payment FROM min_and_max)) THEN 'new'
            ELSE 'active'
          END AS status
    FROM
        max_and_min_customer
)
SELECT
    customer_id
    , min_payment_month
    , max_payment_month
    , status
FROM
    churned_or_new