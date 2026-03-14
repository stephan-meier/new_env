WITH sat_customer_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_customer') }}
),

sat_order_detail_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_product_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_order_detail') }}
),

customers AS (
    SELECT
        hc.customer_hk,
        hc.customer_id,
        sc.last_name,
        sc.first_name,
        sc.company,
        sc.email
    FROM {{ ref('hub_customer') }} hc
    INNER JOIN sat_customer_current sc
        ON hc.customer_hk = sc.customer_hk AND sc.rn = 1
),

order_links AS (
    SELECT
        loc.order_hk,
        loc.customer_hk
    FROM {{ ref('lnk_order_customer') }} loc
),

order_products AS (
    SELECT
        lop.order_hk,
        sod.quantity,
        sod.unit_price,
        sod.discount
    FROM {{ ref('lnk_order_product') }} lop
    INNER JOIN sat_order_detail_current sod
        ON lop.order_product_hk = sod.order_product_hk AND sod.rn = 1
)

SELECT
    c.customer_id,
    c.last_name,
    c.first_name,
    c.company,
    c.email,
    COUNT(DISTINCT ol.order_hk) AS total_orders,
    SUM(op.quantity * op.unit_price * (1 - op.discount / 100)) AS total_revenue
FROM customers c
INNER JOIN order_links ol ON c.customer_hk = ol.customer_hk
INNER JOIN order_products op ON ol.order_hk = op.order_hk
GROUP BY c.customer_id, c.last_name, c.first_name, c.company, c.email
ORDER BY total_revenue DESC
