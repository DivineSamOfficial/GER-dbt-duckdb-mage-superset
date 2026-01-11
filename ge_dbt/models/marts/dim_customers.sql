{{
  config(
    materialized = 'table'
  )
}}

with customers as (

    select *
    from {{ ref('stg_customers') }}

),

enriched_customers as (

    select
        customer_id,
        customer_name,
        gender,
        city,
        state,
        state_code,
        country,
        continent,
        birth_date,

        -- derived attributes
        date_diff('year', birth_date, current_date) as age,

        case
            when birth_date is null then 'Unknown'
            when date_diff('year', birth_date, current_date) < 18 then 'Under 18'
            when date_diff('year', birth_date, current_date) between 18 and 34 then '18–34'
            when date_diff('year', birth_date, current_date) between 35 and 54 then '35–54'
            else '55+'
        end as age_group

    from customers

)

select *
from enriched_customers