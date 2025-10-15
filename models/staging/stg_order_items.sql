with source_data as (

    select
        ORDER_ITEM_ID as order_item_id,
        ORDER_ID as order_id,
        PRODUCT_ID as product_id,
        QUANTITY as quantity,
        UNIT_PRICE as unit_price,
        LINE_TOTAL as line_total,
        _FIVETRAN_SYNCED as fivetran_synced
    from {{ source('google_sheets', 'ORDER_ITEM_SRC') }}

)

select *
from source_data
