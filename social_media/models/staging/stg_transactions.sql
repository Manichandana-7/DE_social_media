{{
    config(
        tags = ["staging","transactions"]
    )
}}

WITH removing_null_rows AS (
    SELECT
        *
    FROM
        {{ source('data_from_snowflake', 'transactions') }}
    WHERE
        customer_id   IS NOT NULL AND
        product_id    IS NOT NULL AND
        payment_month IS NOT NULL AND
        revenue_type  IS NOT NULL AND
        revenue       IS NOT NULL AND
        quantity      IS NOT NULL AND
        dimension_1   IS NOT NULL AND
        dimension_2   IS NOT NULL AND
        dimension_3   IS NOT NULL AND
        dimension_4   IS NOT NULL AND
        dimension_5   IS NOT NULL AND
        dimension_6   IS NOT NULL AND
        dimension_7   IS NOT NULL AND
        dimension_8   IS NOT NULL AND
        dimension_9   IS NOT NULL AND
        dimension_10  IS NOT NULL AND
        companies     IS NOT NULL 
),
removing_duplicates AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY customer_id,product_id,payment_month,revenue_type,revenue,quantity,companies ORDER BY customer_id ASC) AS row_number
    FROM
        removing_null_rows
),
changing_data_type AS (
    SELECT
        CAST(customer_id AS INT)                            AS customer_id
        , LOWER(product_id)                                 AS product_id
        , CAST(TO_DATE(payment_month,'DD-MM-YYYY') AS DATE) AS payment_month
        , CAST(revenue_type AS INT)                         AS revenue_type
        , CAST(revenue AS FLOAT)                            AS revenue
        , CAST(quantity AS INT)                             AS quantity
        , dimension_1
        , dimension_2
        , dimension_3
        , dimension_4
        , dimension_5
        , dimension_6
        , dimension_7
        , dimension_8
        , dimension_9
        , dimension_10
        , companies
    FROM
        removing_duplicates
    WHERE
        row_number = 1
)
SELECT
    *
FROM
    changing_data_type