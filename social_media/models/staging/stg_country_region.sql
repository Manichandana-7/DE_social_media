{{
    config(
        tags = ["staging","country_region"]
    )
}}
WITH removing_null_rows AS (
    SELECT
        *
    FROM
        {{ source('data_from_snowflake', 'countryregion') }}
    WHERE
        customer_id IS NOT NULL AND
        country     IS NOT NULL AND
        region      IS NOT NULL
),
removing_duplicates AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY customer_id,country,region ORDER BY customer_id ASC) AS row_number
    FROM
        removing_null_rows
),
changing_cases AS (
    SELECT
        CAST(customer_id AS INT) AS customer_id
        , LOWER(country)         AS country
        , LOWER(region)          AS region
    FROM
        removing_duplicates
    WHERE
        row_number = 1
)
SELECT
    *
FROM
    changing_cases