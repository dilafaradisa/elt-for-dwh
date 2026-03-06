INSERT INTO final.fact_order_payment (
    order_nk,
    payment_sequential,
    order_purchase_date,
    order_purchase_time,
    payment_type,
    payment_installments,
    payment_value
)

SELECT
    op.order_id AS order_nk,
    op.payment_sequential,
    dd_purchase.date_dim_id AS order_purchase_date,
    dt_purchase.time_id AS order_purchase_time,
    op.payment_type,
    op.payment_installments,
    op.payment_value

FROM stg.order_payments op
JOIN stg.orders o
    ON op.order_id = o.order_id

JOIN final.dim_date dd_purchase
    ON dd_purchase.date_actual = DATE(o.order_purchase_timestamp)

JOIN final.dim_time dt_purchase
    ON dt_purchase.time_actual = DATE_TRUNC('minute', o.order_purchase_timestamp::TIME)

ON CONFLICT (order_nk, payment_sequential)
DO UPDATE SET
    payment_type = EXCLUDED.payment_type,
    payment_installments = EXCLUDED.payment_installments,
    payment_value = EXCLUDED.payment_value,
    updated_at = CURRENT_TIMESTAMP;