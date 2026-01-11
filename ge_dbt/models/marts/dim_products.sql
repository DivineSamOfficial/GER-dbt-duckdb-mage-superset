{{
  config(
    materialized = 'table'
  )
}}

with products as (

    select *
    from {{ ref('stg_products') }}

),

enriched as (

    select
        product_id,
        product_name,
        brand,
        color,
        category_key,
        category,
        subcategory_key,
        subcategory,

        unit_cost,
        unit_price,

        -- derived attributes
        unit_price - unit_cost                       as unit_margin,
        case
            when unit_cost = 0 then null
            else round((unit_price - unit_cost) / unit_cost, 4)
        end                                          as margin_pct

    from products

)

select *
from enriched
