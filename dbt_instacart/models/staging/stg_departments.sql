{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging: Departments from Iceberg Bronze
-- Clean and standardize department reference data

with source as (
    select * from {{ source('iceberg_bronze', 'departments') }}
),

staged as (
    select
        -- Primary key
        cast(department_id as int) as department_id,

        -- Attributes
        trim(department) as department

    from source
    where department_id is not null
)

select * from staged
