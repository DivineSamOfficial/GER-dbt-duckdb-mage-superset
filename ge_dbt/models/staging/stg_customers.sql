{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    incremental_strategy = 'merge'
  )
}}

with source_customer as (

    select *
    from {{ source('lake_view', 'ge_customers_lake_view') }}

),

customers_renamed as (

    select
        cast(CustomerKey as integer) as customer_id,
        trim(Name)                   as customer_name,
        Gender                       as gender,
        City                         as city,
        "State Code"                 AS state_code,  
        "State"                      as state,
        "Zip Code"                   AS zip_code, 
        Country                      as country,
        Continent                    as continent,
        cast(Birthday as date)       as birth_date
    from source_customer

)

select * from customers_renamed
