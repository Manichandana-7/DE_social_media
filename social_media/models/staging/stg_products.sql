{{
    config(
        tags = ["staging","products"]
    )
}}
WITH removing_null_rows AS (
    SELECT
        *
    FROM
        {{ source('data_from_snowflake', 'products') }}
    WHERE
        product_id         IS NOT NULL AND
        product_family     IS NOT NULL AND
        product_sub_family IS NOT NULL
),
removing_duplicates AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY product_id,product_family,product_sub_family ORDER BY product_id ASC) AS row_number
    FROM
        removing_null_rows
),
changing_cases AS (
    SELECT
        LOWER(product_id)           AS product_id
        , LOWER(product_family)     AS product_family
        , LOWER(product_sub_family) AS product_sub_family
    FROM
        removing_duplicates
    WHERE
        row_number = 1
)
SELECT
    *
FROM
    changing_cases