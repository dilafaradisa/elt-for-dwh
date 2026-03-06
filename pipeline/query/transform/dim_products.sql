-- update
UPDATE final.dim_products AS dim
SET
    current_flag = 'N',
    updated_at = CURRENT_TIMESTAMP
FROM stg.products AS stg
WHERE
    dim.product_nk = stg.product_id    
    AND dim.current_flag = 'Y'                
    AND (             
        dim.product_category_name IS DISTINCT FROM stg.product_category_name
        OR dim.product_name_length IS DISTINCT FROM stg.product_name_length
        OR dim.product_description_length IS DISTINCT FROM stg.product_description_length
        OR dim.product_photos_qty IS DISTINCT FROM stg.product_photos_qty
        OR dim.product_weight_g IS DISTINCT FROM stg.product_weight_g
        OR dim.product_length_cm IS DISTINCT FROM stg.product_length_cm
        OR dim.product_height_cm IS DISTINCT FROM stg.product_height_cm
        OR dim.product_width_cm IS DISTINCT FROM stg.product_width_cm
    );

-- insert new record
INSERT INTO final.dim_products (
    product_nk, 
    product_category_name, 
    product_name_length, 
    product_description_length, 
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM stg.products p
WHERE NOT EXISTS (
    SELECT 1
    FROM final.dim_products dim
    WHERE
        dim.product_nk = p.product_id
        AND dim.current_flag = 'Y'
        AND dim.product_category_name IS NOT DISTINCT FROM p.product_category_name
        AND dim.product_name_length IS NOT DISTINCT FROM p.product_name_length
        AND dim.product_description_length IS NOT DISTINCT FROM p.product_description_length
        AND dim.product_photos_qty IS NOT DISTINCT FROM p.product_photos_qty
        AND dim.product_weight_g IS NOT DISTINCT FROM p.product_weight_g
        AND dim.product_length_cm IS NOT DISTINCT FROM p.product_length_cm
        AND dim.product_height_cm IS NOT DISTINCT FROM p.product_height_cm
        AND dim.product_width_cm IS NOT DISTINCT FROM p.product_width_cm
);