-- update there is already a record with same customer_nk and current_flag = 'Y' but some attributes are different

UPDATE final.dim_customers AS dim
SET
    current_flag = 'N',
    updated_at = CURRENT_TIMESTAMP
FROM stg.customers as c
WHERE
    dim.customer_nk = c.customer_id    
    AND dim.current_flag = 'Y'                
    AND (                                     
        dim.customer_unique_id IS DISTINCT FROM c.customer_unique_id
        OR dim.customer_zip_code_prefix IS DISTINCT FROM c.customer_zip_code_prefix
        OR dim.customer_city IS DISTINCT FROM c.customer_city
        OR dim.customer_state IS DISTINCT FROM c.customer_state
    );

-- insert new record if there is no record with same customer_nk and current_flag 'Y'
INSERT INTO final.dim_customers (
    customer_nk, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM stg.customers as c
WHERE NOT EXISTS (
    SELECT 1
    FROM final.dim_customers dim
    WHERE
        dim.customer_nk = c.customer_id
        AND dim.current_flag = 'Y'
        AND dim.customer_unique_id IS NOT DISTINCT FROM c.customer_unique_id
        AND dim.customer_zip_code_prefix IS NOT DISTINCT FROM c.customer_zip_code_prefix
        AND dim.customer_city IS NOT DISTINCT FROM c.customer_city
        AND dim.customer_state IS NOT DISTINCT FROM c.customer_state
);