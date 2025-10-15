with source_data as (

    select
        CUSTOMER_ID as customer_id,
        FIRST_NAME as first_name,
        LAST_NAME as last_name,
        EMAIL as email,
        PHONE as phone,
        CITY as city,
        STATE as state,
        CAST(SIGNUP_DATE as DATE) as signup_date,
        _FIVETRAN_SYNCED as fivetran_synced
    from {{ source('google_sheets', 'CUSTOMER_SRC') }}

)

select *
from source_data
