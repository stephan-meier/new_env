WITH products AS (
    SELECT
        hp.product_hk,
        hp.product_id,
        sp.product_code,
        sp.product_name,
        sp.category,
        sp.list_price,
        sp.standard_cost
    FROM {{ ref('hub_product') }} hp
    INNER JOIN {{ ref('sat_product') }} sp
        ON hp.product_hk = sp.product_hk
),

sales AS (
    SELECT
        lop.product_hk,
        lop.order_hk,
        sod.quantity,
        sod.unit_price,
        sod.discount
    FROM {{ ref('lnk_order_product') }} lop
    INNER JOIN {{ ref('sat_order_detail') }} sod
        ON lop.order_product_hk = sod.order_product_hk
)

SELECT
    p.product_id,
    p.product_code,
    p.product_name,
    p.category,
    p.list_price,
    p.standard_cost,
    COUNT(DISTINCT s.order_hk) AS orders_count,
    SUM(s.quantity) AS total_quantity_sold,
    SUM(s.quantity * s.unit_price * (1 - s.discount / 100)) AS total_revenue,
    SUM(s.quantity * (s.unit_price - p.standard_cost)) AS estimated_margin
FROM products p
LEFT JOIN sales s ON p.product_hk = s.product_hk
GROUP BY p.product_id, p.product_code, p.product_name, p.category, p.list_price, p.standard_cost
ORDER BY total_revenue DESC
