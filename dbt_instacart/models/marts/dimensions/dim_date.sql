{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Date calendar table
-- Note: Instacart dataset doesn't have actual dates, so this is a template

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2017-01-01' as date)",
        end_date="cast('2017-12-31' as date)"
    )}}
),

dim_date as (
    select
        -- Primary key (YYYYMMDD format)
        cast(format_date('%Y%m%d', date_day) as int64) as date_key,
        
        -- Date
        date_day as date,
        
        -- Year
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        
        -- Month
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        format_date('%b', date_day) as month_name_short,
        
        -- Week
        extract(week from date_day) as week_of_year,
        extract(isoweek from date_day) as iso_week,
        
        -- Day
        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week,  -- 1=Sunday, 7=Saturday
        format_date('%A', date_day) as day_name,
        format_date('%a', date_day) as day_name_short,
        
        -- Flags
        case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
        case when extract(day from date_day) = 1 then true else false end as is_month_start,
        case when date_day = last_day(date_day) then true else false end as is_month_end,
        
        -- Fiscal period (adjust based on business needs)
        case
            when extract(month from date_day) in (1, 2, 3) then 'Q1'
            when extract(month from date_day) in (4, 5, 6) then 'Q2'
            when extract(month from date_day) in (7, 8, 9) then 'Q3'
            else 'Q4'
        end as fiscal_quarter
        
    from date_spine
)

select * from dim_date
