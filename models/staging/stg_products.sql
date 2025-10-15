with source_data as (

    select
        PRODUCT_ID as product_id,
        PRODUCT_NAME as product_name,
        CATEGORY as category,
        PRICE as price,
        COST as cost,
        _FIVETRAN_SYNCED as fivetran_synced
    from {{ source('google_sheets', 'PRODUCT_SRC') }}

)

select *
from source_data
