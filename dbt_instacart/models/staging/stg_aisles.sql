{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: Aisles from Iceberg Bronze
-- Clean and standardize aisle reference data

with source as (
    select * from {{ source('iceberg_bronze', 'aisles') }}
),

staged as (
    select
        -- Primary key
        cast(aisle_id as int) as aisle_id,

        -- Attributes
        trim(aisle) as aisle

    from source
    where aisle_id is not null
)

select * from staged
