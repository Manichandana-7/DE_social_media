
WITH customer_sum_revenue AS(
    SELECT
        customer_id
        , SUM(revenue*quantity) AS total_revenue
    FROM
        {{ ref('stg_transactions') }}
    WHERE
        revenue_type = 1
    GROUP BY
        customer_id
),
assign_rank AS(
    SELECT
        customer_id
        , total_revenue
        , RANK() OVER(ORDER BY total_revenue DESC) AS ranking
    FROM
        customer_sum_revenue
)
SELECT
    *
FROM    
    assign_rank