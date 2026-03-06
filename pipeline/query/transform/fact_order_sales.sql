INSERT INTO final.fact_order_sales (
    order_nk,
    customer_id,
    product_id,
    seller_id,
    order_item_id,
    order_purchase_date,
    order_purchase_time,
    order_status,
    price,
    freight_value
)
SELECT
    o.order_id AS order_nk,
    dc.customer_id,
    dp.product_id,
    ds.seller_id,
    oi.order_item_id,
    dd_purchase.date_dim_id AS order_purchase_date,
    dt_purchase.time_id AS order_purchase_time,
    o.order_status,
    oi.price,
    oi.freight_value

FROM stg.order_items oi
JOIN stg.orders o
    ON oi.order_id = o.order_id
JOIN final.dim_customers dc
    ON o.customer_id = dc.customer_nk
    AND dc.current_flag = 'Y'
JOIN final.dim_products dp
    ON oi.product_id = dp.product_nk
    AND dp.current_flag = 'Y'
JOIN final.dim_sellers ds
    ON oi.seller_id = ds.seller_nk
    AND ds.current_flag = 'Y'
JOIN final.dim_date dd_purchase
    ON dd_purchase.date_actual = DATE(o.order_purchase_timestamp)
JOIN final.dim_time dt_purchase
    ON dt_purchase.time_actual = DATE_TRUNC('minute', o.order_purchase_timestamp::TIME)

ON CONFLICT (order_nk, order_item_id)
DO UPDATE SET
    order_status = EXCLUDED.order_status,
    updated_at = CURRENT_TIMESTAMP;