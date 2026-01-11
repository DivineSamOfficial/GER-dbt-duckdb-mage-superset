{{
  config(
    materialized = 'incremental',
    unique_key = 'currency_date_key',
    incremental_strategy = 'merge'
  )
}}

with exchange_rates_source as (

    select *
    from {{ source('lake_view', 'ge_exchange_rates_lake_view') }}

),

exchange_rates_renamed as (

    select
        currency                                    as currency_id,
        cast(Date as date)                          as rate_date,
        cast(Exchange as numeric(12,6))             as exchange_rate,

        -- composite surrogate key
        Currency || '_' || cast(Date as varchar)    as currency_date_key

    from exchange_rates_source

)

select *
from exchange_rates_renamed
