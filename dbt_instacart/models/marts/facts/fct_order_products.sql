{{
    config(
        materialized='table',
        schema='marts',
        partition_by={
            'field': 'order_date',
            'data_type': 'date'
        },
        cluster_by=['user_id', 'product_id']
    )
}}

-- Fact: Order-Product associations
-- Grain: One row per order-product combination

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
        case when op.reordered = 1 then true else false end as is_reordered,
        
        -- Degenerate dimensions
        o.order_number,
        o.order_dow as day_of_week,
        o.order_hour_of_day as hour_of_day,
        o.days_since_prior_order,
        case when o.is_first_order then true else false end as is_first_order,
        
        -- Product attributes (for partitioning/clustering)
        p.department_id,
        p.aisle_id,
        
        -- Derived date (for partitioning)
        date('2017-06-01') as order_date  -- Placeholder: Instacart doesn't have dates
        
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
