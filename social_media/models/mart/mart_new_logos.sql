{{
    config(
        tags = ["mart"]
    )
}}
SELECT
    *
FROM
    {{ ref('int_new_logos') }}