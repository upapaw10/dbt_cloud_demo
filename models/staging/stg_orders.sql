with source_data as (

    select
        ORDER_ID as order_id,
        CUSTOMER_ID as customer_id,
        CAST(ORDER_DATE as DATE) as order_date,
        ORDER_STATUS as order_status,
        TOTAL_AMOUNT as total_amount,
        _FIVETRAN_SYNCED as fivetran_synced
    from {{ source('google_sheets', 'ORDER_SRC') }}

)

select *
from source_data
