{{
  config(
    materialized = 'table'
  )
}}

with stores as (

    select *
    from {{ ref('stg_stores') }}

),

enriched as (

    select
        store_id,
        country,
        state,
        square_meters, 
        open_date,

        -- derived attributes
        date_diff('year', open_date, current_date)   as store_age_years,

        case
            when date_diff('year', open_date, current_date) < 5 then '0–4 years'
            when date_diff('year', open_date, current_date) < 10 then '5–9 years'
            else '10+ years'
        end                                          as store_age_bucket

    from stores

)

select *
from enriched
