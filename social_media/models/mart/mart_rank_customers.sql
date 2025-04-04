{{
    config(
        tags = ["mart"]
    )
}}
SELECT
    *
FROM
    {{ ref('int_rank_customer') }}