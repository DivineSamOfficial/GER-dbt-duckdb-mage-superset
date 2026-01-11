{{
  config(
    materialized = 'incremental',
    unique_key = 'product_id',
    incremental_strategy = 'merge'
  )
}}

with products_source as (

    select *
    from {{ source('lake_view', 'ge_products_lake_view') }}

),

products_remnamed as (

    select
        cast(ProductKey as integer)                  as product_id,
        "Product Name"                               as product_name,
        Brand                                        as brand,
        Color                                        as color,
        CategoryKey                                  as category_key,    
        Category                                     as category,
        SubcategoryKey                               as subcategory_key,
        Subcategory                                  as subcategory,
        try_cast(
            regexp_replace("Unit Cost USD", '[^0-9.]', '', 'g')
            as numeric(10,2)
        )                                            as unit_cost,
        try_cast(
            regexp_replace("Unit Price USD", '[^0-9.]', '', 'g')
            as numeric(10,2)
        )                                            as unit_price        
    from products_source

)

select *
from products_remnamed
