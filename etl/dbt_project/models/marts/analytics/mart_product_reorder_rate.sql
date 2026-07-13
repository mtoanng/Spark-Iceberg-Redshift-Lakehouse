{{
    config(
        materialized='view',
        schema='marts'
    )
}}

-- Mart: Product Reorder Rate
-- Aggregated from fct_order_products — answers "which products get reordered most?"
-- reorder_rate = SUM(reordered) / COUNT(*) grouped by product_id

with fct as (
    select * from {{ ref('fct_order_products') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

product_stats as (
    select
        product_id,
        count(*) as total_order_lines,
        sum(reordered) as reorder_count,
        sum(reordered) / count(*) as reorder_rate
    from fct
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.department,
    p.aisle,
    ps.total_order_lines,
    ps.reorder_count,
    round(ps.reorder_rate, 4) as reorder_rate

from product_stats ps
inner join dim_product p on ps.product_id = p.product_id
order by ps.reorder_rate desc
