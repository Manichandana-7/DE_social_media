{{
    config(
        tags = ["mart"]
    )
}}
SELECT
    *
FROM
    {{ ref('int_crossell') }}