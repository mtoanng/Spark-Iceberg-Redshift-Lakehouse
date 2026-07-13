{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Orders
-- SCD Type 1 (overwrite on change) — no SCD2 per blueprint cut
-- Grain: 1 row per order

with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_orders as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,

        -- Natural key
        order_id,

        -- Order attributes
        user_id,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        eval_set,

        -- Flag: is this the user's first order?
        case when is_first_order then true else false end as is_first_order,

        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from orders
)

select * from dim_orders
