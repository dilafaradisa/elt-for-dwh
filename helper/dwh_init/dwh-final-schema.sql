-- DROP TABLE IF EXISTS fact_delivery_process;
-- DROP TABLE IF EXISTS fact_order_review;
-- DROP TABLE IF EXISTS fact_order_sales;
-- DROP TABLE IF EXISTS dim_time;
-- DROP TABLE IF EXISTS dim_date;
-- DROP TABLE IF EXISTS dim_products;
-- DROP TABLE IF EXISTS dim_sellers;
-- DROP TABLE IF EXISTS dim_customers;

CREATE SCHEMA IF NOT EXISTS final AUTHORIZATION postgres;

-- DIMENSION TABLES

CREATE TABLE IF NOT EXISTS final.dim_customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_nk VARCHAR(255) NOT NULL,
    customer_unique_id VARCHAR(255),
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(255),
    customer_state VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'Y'
);

CREATE TABLE IF NOT EXISTS final.dim_sellers (
    seller_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_nk VARCHAR(255) NOT NULL,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(255),
    seller_state VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'Y'
);

CREATE TABLE IF NOT EXISTS final.dim_products (
    product_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_nk VARCHAR(255) NOT NULL,
    product_category_name VARCHAR(255),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g DECIMAL(10, 2),
    product_length_cm DECIMAL(10, 2),
    product_height_cm DECIMAL(10, 2),
    product_width_cm DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_flag VARCHAR(10) DEFAULT 'Y'
);

CREATE TABLE IF NOT EXISTS final.dim_date (
    date_dim_id INTEGER PRIMARY KEY,
    date_actual DATE,
    day_suffix VARCHAR(4),
    day_name VARCHAR(9),
    day_of_year INTEGER,
    week_of_month INTEGER,
    week_of_year INTEGER,
    week_of_year_iso CHAR(10),
    month_actual INTEGER,
    month_name VARCHAR(9),
    month_name_abbreviated CHAR(3),
    quarter_actual INTEGER,
    quarter_name VARCHAR(9),
    year_actual INTEGER,
    first_day_of_week DATE,
    last_day_of_week DATE,
    first_day_of_month DATE,
    last_day_of_month DATE,
    first_day_of_quarter DATE,
    last_day_of_quarter DATE,
    first_day_of_year DATE,
    last_day_of_year DATE,
    mmyyyy CHAR(6),
    mmddyyyy CHAR(10),
    weekend_indr VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS final.dim_time (
    time_id INTEGER PRIMARY KEY,
    time_actual TIME,
    hours_24 CHAR(2),
    hours_12 CHAR(2),
    hour_minutes CHAR(2),
    day_minutes INTEGER,
    day_time_name VARCHAR(20),
    day_night VARCHAR(20)
);

-- FACT TABLES

CREATE TABLE IF NOT EXISTS final.fact_order_sales (
    order_sales_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_nk VARCHAR(255) NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    order_item_id INTEGER NOT NULL,
    order_purchase_date INTEGER NOT NULL,
    order_purchase_time INTEGER NOT NULL,
    order_status VARCHAR(50),
    price DECIMAL(18, 2),
    freight_value DECIMAL(18, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (order_nk, order_item_id),
    FOREIGN KEY (customer_id) REFERENCES final.dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES final.dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES final.dim_sellers(seller_id),
    FOREIGN KEY (order_purchase_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_purchase_time) REFERENCES final.dim_time(time_id)
);

CREATE TABLE IF NOT EXISTS final.fact_order_payment (
    order_payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_nk VARCHAR(255) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    order_purchase_date INTEGER NOT NULL,
    order_purchase_time INTEGER NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INTEGER NOT NULL,
    payment_value DECIMAL(18, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (order_nk, payment_sequential),
    FOREIGN KEY (order_purchase_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_purchase_time) REFERENCES final.dim_time(time_id)
);

CREATE TABLE IF NOT EXISTS final.fact_order_review (
    order_review_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_nk VARCHAR(255),
    customer_id BIGINT NOT NULL,
    order_purchase_date INTEGER NOT NULL,
    order_purchase_time INTEGER NOT NULL,
    review_nk VARCHAR(255) NOT NULL,
    review_creation_date INTEGER NOT NULL,
    review_score INTEGER,
    review_comment_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (review_nk),
    FOREIGN KEY (customer_id) REFERENCES final.dim_customers(customer_id),
    FOREIGN KEY (order_purchase_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_purchase_time) REFERENCES final.dim_time(time_id),
    FOREIGN KEY (review_creation_date) REFERENCES final.dim_date(date_dim_id)
);

CREATE TABLE IF NOT EXISTS final.fact_delivery_process (
    delivery_process_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_nk VARCHAR(255),
    product_id INTEGER,
    seller_id INTEGER NOT NULL,
    order_purchase_date INTEGER NOT NULL,
    order_purchase_time INTEGER NOT NULL,
    order_approved_date INTEGER,
    order_approved_time INTEGER,
    order_delivered_carrier_date INTEGER,
    order_delivered_carrier_time INTEGER,
    order_delivered_customer_date INTEGER,
    order_delivered_customer_time INTEGER,
    order_estimated_delivery_date INTEGER,
    seller_to_carrier_duration_days INTEGER,
    carrier_to_cust_duration_days INTEGER,
    order_to_delivery_duration_days INTEGER,
    delay_delivery_flag CHAR(1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (order_nk, seller_id),
    FOREIGN KEY (product_id) REFERENCES final.dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES final.dim_sellers(seller_id),
    FOREIGN KEY (order_purchase_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_purchase_time) REFERENCES final.dim_time(time_id),
    FOREIGN KEY (order_approved_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_approved_time) REFERENCES final.dim_time(time_id),
    FOREIGN KEY (order_delivered_carrier_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_delivered_carrier_time) REFERENCES final.dim_time(time_id),
    FOREIGN KEY (order_delivered_customer_date) REFERENCES final.dim_date(date_dim_id),
    FOREIGN KEY (order_delivered_customer_time) REFERENCES final.dim_time(time_id),
    FOREIGN KEY (order_estimated_delivery_date) REFERENCES final.dim_date(date_dim_id)
);

INSERT INTO final.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
WHERE NOT EXISTS (SELECT 1 FROM final.dim_date LIMIT 1)
ORDER BY 1;

INSERT INTO final.dim_time
SELECT  
	cast(to_char(minute, 'hh24mi') as numeric) time_id,
	to_char(minute, 'hh24:mi')::time AS tume_actual,
	-- Hour of the day (0 - 23)
	to_char(minute, 'hh24') AS hour_24,
	-- Hour of the day (0 - 11)
	to_char(minute, 'hh12') hour_12,
	-- Hour minute (0 - 59)
	to_char(minute, 'mi') hour_minutes,
	-- Minute of the day (0 - 1439)
	extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '00:00' AND '11:59'
		then 'AM'
		when to_char(minute, 'hh24:mi') BETWEEN '12:00' AND '23:59'
		then 'PM'
	end AS day_time_name,
	-- Indicator of day or night
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '07:00' AND '19:59' then 'Day'	
		else 'Night'
	end AS day_night
FROM 
	(SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute 
	FROM  generate_series(0,1439) AS sequence(minute)
GROUP BY sequence.minute
) DQ
WHERE NOT EXISTS (SELECT 1 FROM final.dim_time LIMIT 1)
ORDER BY 1;

