WITH source AS (
    SELECT * FROM {{ source('bronze', 'erp_px_cat_g1v2') }}
)

SELECT
    id,
    cat,
    subcat,
    maintenance
FROM source