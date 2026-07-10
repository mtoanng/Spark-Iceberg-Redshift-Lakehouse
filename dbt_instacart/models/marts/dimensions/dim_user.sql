{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Users with aggregated metrics
-- SCD Type 1 (overwrite on change)

with users as (
    select * from {{ ref('stg_users') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

user_metrics as (
    select
        user_id,
        count(distinct order_id) as total_orders_calc,
        min(order_number) as first_order_number,
        max(order_number) as last_order_number,
        avg(order_dow) as avg_order_dow,
        avg(order_hour_of_day) as avg_order_hour,
        avg(days_since_prior_order) as avg_days_between_orders
    from orders
    group by user_id
),

dim_user as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as user_key,
        
        -- Natural key
        u.user_id,
        
        -- User metrics (from pre-computed summary)
        u.total_orders,
        u.avg_basket_size,
        u.reorder_rate,
        u.user_segment,
        
        -- User behavior (from orders)
        m.avg_order_dow,
        m.avg_order_hour,
        m.avg_days_between_orders,
        
        -- User classification
        case
            when u.total_orders = 1 then 'First Time'
            when u.total_orders between 2 and 5 then 'Casual'
            when u.total_orders between 6 and 15 then 'Regular'
            when u.total_orders > 15 then 'Loyal'
            else 'Unknown'
        end as user_loyalty_tier,
        
        case
            when u.reorder_rate >= 0.7 then 'High Repeater'
            when u.reorder_rate >= 0.4 then 'Medium Repeater'
            when u.reorder_rate >= 0.2 then 'Low Repeater'
            else 'Non Repeater'
        end as reorder_behavior,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from users u
    left join user_metrics m on u.user_id = m.user_id
)

select * from dim_user
