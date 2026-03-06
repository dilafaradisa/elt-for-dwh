-- update
UPDATE final.dim_sellers AS dim
SET
    current_flag = 'N',
    updated_at = CURRENT_TIMESTAMP
FROM stg.sellers AS stg
WHERE
    dim.seller_nk = stg.seller_id    
    AND dim.current_flag = 'Y'                
    AND (             
        dim.seller_zip_code_prefix IS DISTINCT FROM stg.seller_zip_code_prefix
        OR dim.seller_city IS DISTINCT FROM stg.seller_city
        OR dim.seller_state IS DISTINCT FROM stg.seller_state
    );

-- insert new record
INSERT INTO final.dim_sellers (
    seller_nk, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM stg.sellers as stg
WHERE NOT EXISTS (
    SELECT 1
    FROM final.dim_sellers dim
    WHERE
        dim.seller_nk = stg.seller_id
        AND dim.current_flag = 'Y'
        AND dim.seller_zip_code_prefix IS NOT DISTINCT FROM stg.seller_zip_code_prefix
        AND dim.seller_city IS NOT DISTINCT FROM stg.seller_city
        AND dim.seller_state IS NOT DISTINCT FROM stg.seller_state
);