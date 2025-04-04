{{
    config(
        tags = ["mart"]
    )
}}
SELECT
    *
FROM
    {{ ref('int_grr_nrr') }}