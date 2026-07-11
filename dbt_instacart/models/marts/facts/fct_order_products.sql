{{
    config(
        materialized='table',
        schema='marts',
        cluster_by=['user_id', 'product_id']
    )
}}

-- Fact: Order-Product associations
-- Grain: One row per (order_id, product_id) combination
-- This is the primary fact table — built directly from staging, not from other facts.

with order_products as (
    select * from {{ ref('stg_order_products') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

fact_base as (
    select
        -- Natural keys
        op.order_id,
        op.product_id,
        o.user_id,

        -- Measures
        op.add_to_cart_order as cart_sequence,
        cast(op.reordered as int) as reordered,

        -- Degenerate dimensions (from orders)
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        o.is_first_order,

        -- Product attributes (for clustering / filtering)
        p.department_id,
        p.aisle_id

    from order_products op
    inner join orders o on op.order_id = o.order_id
    inner join products p on op.product_id = p.product_id
),

fct_order_products as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_product_key,

        *

    from fact_base
)

select * from fct_order_products
