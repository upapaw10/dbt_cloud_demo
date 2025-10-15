{{ config(
    materialized='incremental',
    transient=false,
    unique_key='PRODUCT_ID',
    incremental_strategy='merge'
) }}

WITH src AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY,
        COST,
        PRICE,
        FIVETRAN_SYNCED AS SOURCE_UPDATED_AT,
        MD5(CONCAT_WS('|', PRODUCT_NAME, CATEGORY, COST, PRICE)) AS HASH_KEY,
        CURRENT_TIMESTAMP() AS RECORD_START_DATE,
        NULL AS RECORD_END_DATE,
        TRUE AS IS_ACTIVE
    FROM {{ ref('stg_products') }}
),

changes AS (
    SELECT
        s.*
    FROM src s
    {% if is_incremental() %}
        LEFT JOIN {{ this }} t
            ON s.PRODUCT_ID = t.PRODUCT_ID
            AND t.IS_ACTIVE = TRUE
        WHERE t.HASH_KEY IS NULL OR t.HASH_KEY <> s.HASH_KEY
    {% endif %}
)

SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    COST,
    PRICE,
    SOURCE_UPDATED_AT,
    HASH_KEY,
    RECORD_START_DATE,
    RECORD_END_DATE,
    IS_ACTIVE
FROM changes

{% if is_incremental() %}
UNION ALL
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    COST,
    PRICE,
    SOURCE_UPDATED_AT,
    HASH_KEY,
    RECORD_START_DATE,
    RECORD_END_DATE,
    IS_ACTIVE
FROM {{ this }}
WHERE IS_ACTIVE = FALSE
{% endif %}
