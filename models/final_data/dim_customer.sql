{{ config(
    materialized='incremental',
    transient=false,
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge',
    on_schema_change='ignore'
) }}

WITH src AS (
    SELECT
        CUSTOMER_ID,
        INITCAP(CONCAT(FIRST_NAME, ' ', LAST_NAME)) AS CUSTOMER_NAME,
        EMAIL,
        PHONE,
        CITY,
        STATE,
        SIGNUP_DATE,
        MD5(CONCAT_WS('|', FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, STATE)) AS HASH_KEY,
        CURRENT_TIMESTAMP() AS RECORD_START_DATE,
        NULL AS RECORD_END_DATE,
        TRUE AS IS_ACTIVE
    FROM {{ ref('stg_customers') }}
),

-- Identify changed or new records
changes AS (
    SELECT
        s.*
    FROM src s
    {% if is_incremental() %}
        LEFT JOIN {{ this }} t
            ON s.CUSTOMER_ID = t.CUSTOMER_ID
            AND t.IS_ACTIVE = TRUE
        WHERE t.HASH_KEY IS NULL OR t.HASH_KEY <> s.HASH_KEY
    {% endif %}
)

-- Final union: insert new or changed records, plus keep old inactive ones
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    PHONE,
    CITY,
    STATE,
    SIGNUP_DATE,
    HASH_KEY,
    RECORD_START_DATE,
    RECORD_END_DATE,
    IS_ACTIVE
FROM changes

{% if is_incremental() %}
UNION ALL
-- Keep historical inactive records
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    PHONE,
    CITY,
    STATE,
    SIGNUP_DATE,
    HASH_KEY,
    RECORD_START_DATE,
    RECORD_END_DATE,
    IS_ACTIVE
FROM {{ this }}
WHERE IS_ACTIVE = FALSE
{% endif %}
