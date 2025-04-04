{{
    config(
        tags = ["mart"]
    )
}}
SELECT
    *
FROM
    {{ ref('int_contractions') }}