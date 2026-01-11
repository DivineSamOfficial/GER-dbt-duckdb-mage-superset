{{
  config(
    materialized = 'incremental',
    unique_key = 'store_id',
    incremental_strategy = 'merge'
  )
}}

with stores_source as (

    select *
    from {{ source('lake_view', 'ge_stores_lake_view') }}

),

stores_renamed as (

    select
        cast(StoreKey as integer)      as store_id,
        State                          as state,
        Country                        as country,
        cast("Square Meters" as INT)   as square_meters,
        cast("Open Date" as date)      as open_date
    from stores_source

)

select *
from stores_renamed
