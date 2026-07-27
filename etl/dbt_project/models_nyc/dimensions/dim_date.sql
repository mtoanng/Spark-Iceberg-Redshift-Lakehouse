{{ config(materialized='table') }}

with dates as (
    select distinct pickup_date as calendar_date
    from {{ source('silver', 'silver_trips') }}
    where pickup_date is not null
)

select
    cast(to_char(calendar_date, 'YYYYMMDD') as integer) as date_key,
    calendar_date,
    cast(extract(year from calendar_date) as integer) as calendar_year,
    cast(extract(month from calendar_date) as integer) as calendar_month,
    cast(extract(day from calendar_date) as integer) as day_of_month,
    cast(extract(dow from calendar_date) as integer) + 1 as day_of_week,
    trim(to_char(calendar_date, 'Dy')) as day_name
from dates
