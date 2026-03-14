WITH sat_order_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_order') }}
),

sat_customer_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_customer') }}
),

sat_employee_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY employee_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_employee') }}
),

sat_order_detail_current AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_product_hk ORDER BY load_datetime DESC) AS rn
    FROM {{ ref('sat_order_detail') }}
),

orders AS (
    SELECT
        ho.order_hk,
        ho.order_id,
        so.order_date,
        so.shipped_date,
        so.order_status,
        so.payment_type,
        so.shipping_fee
    FROM {{ ref('hub_order') }} ho
    INNER JOIN sat_order_current so
        ON ho.order_hk = so.order_hk AND so.rn = 1
),

customer_link AS (
    SELECT
        loc.order_hk,
        loc.customer_hk
    FROM {{ ref('lnk_order_customer') }} loc
),

customers AS (
    SELECT
        hc.customer_hk,
        sc.last_name || ', ' || sc.first_name AS customer_name,
        sc.company
    FROM {{ ref('hub_customer') }} hc
    INNER JOIN sat_customer_current sc
        ON hc.customer_hk = sc.customer_hk AND sc.rn = 1
),

employee_link AS (
    SELECT
        loe.order_hk,
        loe.employee_hk
    FROM {{ ref('lnk_order_employee') }} loe
),

employees AS (
    SELECT
        he.employee_hk,
        se.last_name || ', ' || se.first_name AS employee_name
    FROM {{ ref('hub_employee') }} he
    INNER JOIN sat_employee_current se
        ON he.employee_hk = se.employee_hk AND se.rn = 1
),

order_items AS (
    SELECT
        lop.order_hk,
        COUNT(*) AS line_items,
        SUM(sod.quantity * sod.unit_price * (1 - sod.discount / 100)) AS order_total
    FROM {{ ref('lnk_order_product') }} lop
    INNER JOIN sat_order_detail_current sod
        ON lop.order_product_hk = sod.order_product_hk AND sod.rn = 1
    GROUP BY lop.order_hk
)

SELECT
    o.order_id,
    o.order_date,
    o.shipped_date,
    o.order_status,
    o.payment_type,
    o.shipping_fee,
    c.customer_name,
    c.company,
    e.employee_name,
    oi.line_items,
    oi.order_total
FROM orders o
LEFT JOIN customer_link cl ON o.order_hk = cl.order_hk
LEFT JOIN customers c ON cl.customer_hk = c.customer_hk
LEFT JOIN employee_link el ON o.order_hk = el.order_hk
LEFT JOIN employees e ON el.employee_hk = e.employee_hk
LEFT JOIN order_items oi ON o.order_hk = oi.order_hk
ORDER BY o.order_date DESC
