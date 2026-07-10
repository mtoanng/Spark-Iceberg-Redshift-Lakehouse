{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: Products hierarchy from Iceberg Silver

with source as (
    select * from {{ source('iceberg_silver', 'products_hierarchy') }}
),

staged as (
    select
        -- Primary key
        product_id,
        
        -- Product attributes
        product_name,
        product_name_clean,
        
        -- Hierarchy
        aisle_id,
        aisle,
        department_id,
        department,
        
        -- Derived attributes
        is_organic,
        is_gluten_free,
        
        -- Metadata
        ingestion_timestamp as source_loaded_at
        
    from source
)

select * from staged
