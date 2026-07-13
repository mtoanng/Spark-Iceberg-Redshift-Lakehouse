{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: Orders from AWS Glue Catalog Silver
-- Clean and standardize orders data

with source as (
    select * from {{ source('glue_silver', 'orders_enriched') }}
),

staged as (
    select
        -- Primary key
        order_id,
        user_id,
        
        -- Order attributes
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        eval_set,
        
        -- User metrics (from enrichment)
        user_total_orders,
        user_first_order_number,
        user_last_order_number,
        is_first_order,
        
        -- Metadata
        ingestion_timestamp as source_loaded_at
        
    from source
)

select * from staged
