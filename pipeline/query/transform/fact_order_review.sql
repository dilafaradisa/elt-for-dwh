INSERT INTO final.fact_order_review (
    order_nk,
    customer_id,
    order_purchase_date,
    order_purchase_time,
    review_nk,
    review_creation_date,
    review_score,
    review_comment_message
)

SELECT
    o.order_id AS order_nk,
    dc.customer_id,
    dd_purchase.date_dim_id AS order_purchase_date,
    dt_purchase.time_id AS order_purchase_time,
    r.review_id AS review_nk,
    dd_review.date_dim_id AS review_creation_date,
    r.review_score,
    r.review_comment_message

FROM stg.order_reviews r
JOIN stg.orders o
    ON r.order_id = o.order_id
JOIN final.dim_customers dc
    ON o.customer_id = dc.customer_nk
    AND dc.current_flag = 'Y'
JOIN final.dim_date dd_purchase
    ON dd_purchase.date_actual = DATE(o.order_purchase_timestamp)  
JOIN final.dim_time dt_purchase
    ON dt_purchase.time_actual = DATE_TRUNC('minute', o.order_purchase_timestamp::TIME) 
JOIN final.dim_date dd_review
    ON dd_review.date_actual = DATE(r.review_creation_date)   

ON CONFLICT (review_nk)
DO UPDATE SET
    review_score = EXCLUDED.review_score,
    review_comment_message = EXCLUDED.review_comment_message,
    updated_at = CURRENT_TIMESTAMP;