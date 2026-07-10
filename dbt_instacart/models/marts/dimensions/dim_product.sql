{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Products with full hierarchy
-- SCD Type 1 (overwrite on change)

with products as (
    select * from {{ ref('stg_products') }}
),

dim_product as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        
        -- Natural key
        product_id,
        
        -- Product attributes
        product_name,
        product_name_clean,
        is_organic,
        is_gluten_free,
        
        -- Hierarchy (denormalized for query performance)
        aisle_id,
        aisle,
        department_id,
        department,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from products
)

select * from dim_product
