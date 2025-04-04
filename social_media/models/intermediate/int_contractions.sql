WITH contraction_revenue AS(
    SELECT
        product_id
        , revenue
        , payment_month
        , LAG(revenue) OVER(ORDER BY payment_month) AS previous_revenue
    FROM
        {{ref('stg_transactions')}}  
),
loss_column AS(
    SELECT
        *,
        CASE
            WHEN previous_revenue>revenue THEN previous_revenue-revenue
            ELSE 0
        END AS loss
    FROM
        contraction_revenue
)
SELECT
    product_id
    , payment_month
    , SUM(loss) AS cumulative_loss
FROM
    loss_column
GROUP BY
    product_id
    , payment_month
ORDER BY
    payment_month
    , product_id