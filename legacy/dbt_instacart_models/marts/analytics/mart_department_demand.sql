{{
    config(
        materialized='view',
        schema='marts'
    )
}}

-- Mart: Department Demand by Time
-- Aggregated from fct_order_products — answers "which department is most popular
-- on which day of week / hour of day?"

with fct as (
    select * from {{ ref('fct_order_products') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

department_volume as (
    select
        p.department,
        fct.order_dow,
        fct.order_hour_of_day,
        count(*) as order_line_count,
        sum(fct.reordered) as reordered_count
    from fct
    inner join dim_product p on fct.product_id = p.product_id
    group by
        p.department,
        fct.order_dow,
        fct.order_hour_of_day
)

select
    department,
    order_dow,
    order_hour_of_day,
    order_line_count,
    reordered_count,
    round(reordered_count / nullif(order_line_count, 0), 4) as reorder_rate
from department_volume
order by order_line_count desc
