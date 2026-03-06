-- truncate all tables before loading data

TRUNCATE TABLE stg.geolocation CASCADE;
TRUNCATE TABLE stg.customers CASCADE ;
TRUNCATE TABLE stg.sellers CASCADE;
TRUNCATE TABLE stg.products CASCADE;
TRUNCATE TABLE stg.product_category_name_translation CASCADE;
TRUNCATE TABLE stg.orders CASCADE;
TRUNCATE TABLE stg.order_items CASCADE;
TRUNCATE TABLE stg.order_payments CASCADE;
TRUNCATE TABLE stg.order_reviews CASCADE;