{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: Order-Product associations from Iceberg Silver

with source as (
    select * from {{ source('iceberg_silver', 'order_products_enriched') }}
),

staged as (
    select
        -- Composite key
        order_id,
        product_id,
        
        -- Product attributes
        product_name,
        aisle_id,
        aisle,
        department_id,
        department,
        
        -- Order attributes
        add_to_cart_order,
        reordered,
        eval_set,
        
        -- Metadata
        silver_timestamp as source_loaded_at
        
    from source
)

select * from staged
