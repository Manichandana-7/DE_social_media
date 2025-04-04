{{
    config(
        tags=["staging", "customers"]
    )
}}

WITH removing_null_rows AS (
    SELECT
        *
    FROM
        {{ source('data_from_snowflake', 'customer') }}
    WHERE
        company      IS NOT NULL AND
        customer_id  IS NOT NULL AND
        customername IS NOT NULL 
),
removing_duplicates AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id ASC) AS row_number
    FROM
        removing_null_rows
),
changing_data_type AS (
    SELECT
        company
        , CAST(customer_id AS INT) AS customer_id
        , LOWER(customername)      AS customer_name
    FROM
        removing_duplicates
    WHERE
        row_number = 1
)
SELECT
    *
FROM
    changing_data_type