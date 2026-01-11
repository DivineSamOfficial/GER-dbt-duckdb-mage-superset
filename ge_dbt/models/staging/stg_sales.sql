{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = 'sale_id'
  )
}}

with sales_source as (

    select *
    from {{ source('lake_view', 'ge_sales_lake_view') }}

),

sales_renamed as (

    select
        cast("Order Number" as integer)       as order_id,
        cast("Line Item" as integer)          as line_item,
        cast("Order Date" as date)            as order_date,
        cast("Delivery Date" as date)         as delivery_date,
        cast(CustomerKey as integer)          as customer_id,
        cast(ProductKey as integer)           as product_id,
        cast(StoreKey as integer)             as store_id,
        cast(Quantity as integer)             as quantity,
        "Currency Code"                       as currency_id

    from sales_source

)

select *
from sales_renamed

{% if is_incremental() %}
-- append only new events
where order_date > (select max(order_date) from {{ this }})
{% endif %}
