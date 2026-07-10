{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: User summary metrics from Iceberg Silver

with source as (
    select * from {{ source('iceberg_silver', 'user_order_summary') }}
),

staged as (
    select
        -- Primary key
        user_id,
        
        -- User metrics
        total_orders,
        avg_basket_size,
        reorder_rate,
        max_order_number,
        
        -- User segmentation
        user_segment
        
    from source
)

select * from staged
