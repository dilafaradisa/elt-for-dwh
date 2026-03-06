INSERT INTO final.fact_delivery_process (
    order_nk,
    seller_id,
    order_purchase_date,
    order_purchase_time,
    order_approved_date,
    order_approved_time,
    order_delivered_carrier_date,
    order_delivered_carrier_time,
    order_delivered_customer_date,
    order_delivered_customer_time,
    order_estimated_delivery_date,
    seller_to_carrier_duration_days,
    carrier_to_cust_duration_days,
    order_to_delivery_duration_days,
    delay_delivery_flag
)
SELECT DISTINCT ON (o.order_id, oi.seller_id)
    o.order_id AS order_nk,
    ds.seller_id,
    dd_purchase.date_dim_id AS order_purchase_date,
    dt_purchase.time_id AS order_purchase_time,
    dd_approved.date_dim_id AS order_approved_date,
    dt_approved.time_id AS order_approved_time,
    dd_carrier.date_dim_id AS order_delivered_carrier_date,
    dt_carrier.time_id AS order_delivered_carrier_time,
    dd_customer.date_dim_id AS order_delivered_customer_date,
    dt_customer.time_id AS order_delivered_customer_time,
    dd_estimated.date_dim_id AS order_estimated_delivery_date,

    CASE 
        WHEN o.order_delivered_carrier_date IS NOT NULL
        THEN EXTRACT(DAY FROM (o.order_delivered_carrier_date::TIMESTAMP - o.order_purchase_timestamp::TIMESTAMP))::INTEGER
    ELSE NULL
    END AS seller_to_carrier_duration_days,

    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_delivered_carrier_date IS NOT NULL
        THEN EXTRACT(DAY FROM (o.order_delivered_customer_date::TIMESTAMP - o.order_delivered_carrier_date::TIMESTAMP))::INTEGER
    ELSE NULL
    END AS carrier_to_cust_duration_days,

    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN EXTRACT(DAY FROM (o.order_delivered_customer_date::TIMESTAMP - o.order_purchase_timestamp::TIMESTAMP))::INTEGER
    ELSE NULL
    END AS order_to_delivery_duration_days,

    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 'Y' ELSE 'N'
    END AS delay_delivery_flag

FROM stg.orders o
JOIN stg.order_items oi
    ON oi.order_id = o.order_id
JOIN final.dim_sellers ds
    ON ds.seller_nk = oi.seller_id
    AND ds.current_flag = 'Y'

-- purchase 
JOIN final.dim_date dd_purchase
    ON dd_purchase.date_actual = DATE(o.order_purchase_timestamp)
JOIN final.dim_time dt_purchase
    ON dt_purchase.time_actual = DATE_TRUNC('minute', o.order_purchase_timestamp::TIME)

-- approved
LEFT JOIN final.dim_date dd_approved
    ON dd_approved.date_actual = DATE(o.order_approved_at) 
LEFT JOIN final.dim_time dt_approved
    ON dt_approved.time_actual = DATE_TRUNC('minute', o.order_approved_at::TIME) 

-- delivered to carrier
LEFT JOIN final.dim_date dd_carrier
    ON dd_carrier.date_actual = DATE(o.order_delivered_carrier_date) 
LEFT JOIN final.dim_time dt_carrier
    ON dt_carrier.time_actual = DATE_TRUNC('minute', o.order_delivered_carrier_date::TIME)

-- delivered to customer
LEFT JOIN final.dim_date dd_customer
    ON dd_customer.date_actual = DATE(o.order_delivered_customer_date)
LEFT JOIN final.dim_time dt_customer
    ON dt_customer.time_actual = DATE_TRUNC('minute', o.order_delivered_customer_date::TIME)

-- estimated delivery
LEFT JOIN final.dim_date dd_estimated
    ON dd_estimated.date_actual = DATE(o.order_estimated_delivery_date) 

ORDER BY o.order_id, oi.seller_id, oi.order_item_id

ON CONFLICT (order_nk, seller_id)
DO UPDATE SET
    order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
    order_delivered_carrier_time = EXCLUDED.order_delivered_carrier_time,
    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
    order_delivered_customer_time = EXCLUDED.order_delivered_customer_time,
    seller_to_carrier_duration_days = EXCLUDED.seller_to_carrier_duration_days,
    carrier_to_cust_duration_days = EXCLUDED.carrier_to_cust_duration_days,
    order_to_delivery_duration_days = EXCLUDED.order_to_delivery_duration_days,
    delay_delivery_flag = EXCLUDED.delay_delivery_flag,
    updated_at = CURRENT_TIMESTAMP;