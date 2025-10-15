{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ORDER_ITEM_ID',
    transient=false,
    on_schema_change='ignore'
) }}

-- STEP 1: Combine order headers with order items
WITH src AS (
    SELECT
        oi.ORDER_ITEM_ID,
        o.ORDER_ID,
        o.CUSTOMER_ID,
        oi.PRODUCT_ID,
        o.ORDER_DATE,
        oi.QUANTITY,
        oi.UNIT_PRICE,
        oi.LINE_TOTAL AS TOTAL_AMOUNT,
        o.ORDER_STATUS,
        GREATEST(o.fivetran_synced, oi.fivetran_synced) AS SOURCE_UPDATED_AT,
        MD5(CONCAT_WS('|',
            o.ORDER_ID,
            o.CUSTOMER_ID,
            oi.PRODUCT_ID,
            o.ORDER_DATE,
            oi.QUANTITY,
            oi.UNIT_PRICE,
            o.ORDER_STATUS
        )) AS HASH_KEY,
        CURRENT_TIMESTAMP() AS RECORD_START_DATE,
        NULL AS RECORD_END_DATE,
        TRUE AS IS_ACTIVE,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_order_items') }} oi
        ON o.ORDER_ID = oi.ORDER_ID
),

-- STEP 2: Join with dimension tables
joined AS (
    SELECT
        s.ORDER_ITEM_ID,
        s.ORDER_ID,
        s.CUSTOMER_ID,
        c.HASH_KEY AS CUSTOMER_HASH_KEY,
        s.PRODUCT_ID,
        p.HASH_KEY AS PRODUCT_HASH_KEY,
        s.ORDER_DATE,
        d.DATE_KEY AS ORDER_DATE_KEY,
        s.QUANTITY,
        s.UNIT_PRICE,
        s.TOTAL_AMOUNT,
        s.ORDER_STATUS,
        s.SOURCE_UPDATED_AT,
        s.HASH_KEY,
        s.RECORD_START_DATE,
        s.RECORD_END_DATE,
        s.IS_ACTIVE,
        s.DBT_UPDATED_AT
    FROM src s
    LEFT JOIN {{ ref('dim_customer') }} c
        ON s.CUSTOMER_ID = c.CUSTOMER_ID AND c.IS_ACTIVE = TRUE
    LEFT JOIN {{ ref('dim_product') }} p
        ON s.PRODUCT_ID = p.PRODUCT_ID AND p.IS_ACTIVE = TRUE
    LEFT JOIN {{ ref('dim_date') }} d
        ON TO_DATE(s.ORDER_DATE) = d.full_date
),

-- STEP 3: Detect new or changed records
changes AS (
    {% if execute and is_incremental() %}
        SELECT
            s.*
        FROM joined s
        LEFT JOIN {{ this }} t
            ON s.ORDER_ITEM_ID = t.ORDER_ITEM_ID
            AND t.IS_ACTIVE = TRUE
        WHERE t.HASH_KEY IS NULL OR t.HASH_KEY <> s.HASH_KEY
    {% else %}
        SELECT * FROM joined
    {% endif %}
)

-- STEP 4: Final output
SELECT * FROM changes

{% if is_incremental() %}
UNION ALL
SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    CUSTOMER_ID,
    CUSTOMER_HASH_KEY,
    PRODUCT_ID,
    PRODUCT_HASH_KEY,
    ORDER_DATE,
    ORDER_DATE_KEY,
    QUANTITY,
    UNIT_PRICE,
    TOTAL_AMOUNT,
    ORDER_STATUS,
    SOURCE_UPDATED_AT,
    HASH_KEY,
    RECORD_START_DATE,
    RECORD_END_DATE,
    IS_ACTIVE,
    DBT_UPDATED_AT
FROM {{ this }}
WHERE IS_ACTIVE = FALSE
{% endif %}
