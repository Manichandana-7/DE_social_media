{{
    config(
        tags = ["intermediate","new_logos"]
    )
}}

WITH customer_first_appearance AS (
    SELECT 
        customer_id
        , MIN(EXTRACT(YEAR FROM payment_month)) AS first_year
    FROM 
        {{ ref('stg_transactions') }}
    GROUP BY 
        customer_id
)
SELECT 
    first_year
    , COUNT(DISTINCT customer_id) AS new_customers
FROM 
    customer_first_appearance
GROUP BY 
    first_year