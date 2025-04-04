
WITH sum_revenue AS(
    SELECT
        product_id
        , SUM(revenue*quantity) AS total_revenue
    FROM
        {{ ref('stg_transactions') }}
    WHERE
        revenue_type = 1
    GROUP BY
        product_id
),
rank_and_join AS(
    SELECT
        product_id
        , total_revenue
        , RANK() OVER(ORDER BY total_revenue DESC) AS ranking
    FROM
        sum_revenue s
)

SELECT
    
    *
FROM
    rank_and_join