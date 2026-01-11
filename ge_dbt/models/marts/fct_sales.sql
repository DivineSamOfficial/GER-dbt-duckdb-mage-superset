{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = 'sale_id'
  )
}}

with sales as (

    select *
    from {{ ref('stg_sales') }}

),

customers as (

    select customer_id,
           customer_name,
           age,
           age_group,
           gender,
           city,
           state_code,
           state,
           country,
           birth_date

    from {{ ref('dim_customers') }}

),

products as (

    select product_id,
           product_name,
           brand,
           color,
           category,
           subcategory,
           unit_cost,
           unit_price,
           unit_margin,
           margin_pct

    from {{ ref('dim_products') }}

),

stores as (

    select store_id,
           country,
           state,
           square_meters,
           open_date,
           store_age_years,
           store_age_bucket  
    from {{ ref('dim_stores') }}

),

exchange_rates as (

    select
        currency_id,
        rate_date,
        exchange_rate
    from {{ ref('stg_exchange_rates') }}

),

enriched as (

    select
        s.order_id,
        s.order_date,
        s.line_item,
        s.delivery_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.quantity,                    
        s.currency_id,
        cu.customer_name,
        cu.gender,
        cu.city AS customer_city,
        cu.state AS customer_state,
        cu.country AS customer_country,
        cu.birth_date AS customer_birthday,
        cu.age AS customer_age,
        cu.age_group AS customer_age_group,
        pr.product_name,
        pr.brand,
        pr.color,
        pr.category,
        pr.subcategory,
        pr.unit_cost,
        pr.unit_price,
        pr.unit_cost * s.quantity AS total_cost,
        pr.unit_price * s.quantity AS total_price,
        st.state AS store_state,
        st.country AS store_country,
        st.square_meters AS store_square_meters,
        st.open_date AS store_open_date,
        st.store_age_years,
        st.store_age_bucket,
        er.rate_date,
        er.exchange_rate

    from sales s
    left join customers cu
        on s.customer_id = cu.customer_id
    left join products pr
        on s.product_id = pr.product_id
    left join stores st
        on s.store_id = st.store_id
    left join exchange_rates er
        on s.currency_id = er.currency_id
       and s.order_date = er.rate_date

)

select *
from enriched

{% if is_incremental() %}
-- append only new sales
where order_date > (select max(order_date) from {{ this }})
{% endif %}
