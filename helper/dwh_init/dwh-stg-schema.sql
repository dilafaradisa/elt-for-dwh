CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id text NOT NULL PRIMARY KEY,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.geolocation (
    geolocation_zip_code_prefix integer NOT NULL PRIMARY KEY,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.order_items (
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price real,
    freight_value real,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp,
    primary key (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS stg.order_payments (
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value real,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp,
    primary key (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS stg.order_reviews (
    review_id text NOT NULL PRIMARY KEY,
    order_id text NOT NULL,
    review_score integer,
    review_comment_title text,
    review_comment_message text, 
    review_creation_date text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.orders (
    order_id text NOT NULL PRIMARY KEY,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.product_category_name_translation (
    product_category_name text NOT NULL,
    product_category_name_english text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.products (
    product_id text NOT NULL PRIMARY KEY,
    product_category_name text,
    product_name_length real,
    product_description_length real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stg.sellers (
    seller_id text NOT NULL PRIMARY KEY,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);