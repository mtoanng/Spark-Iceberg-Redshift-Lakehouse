{{ config(materialized='table', file_format='iceberg') }}

with dates as (
    select distinct pickup_date as calendar_date
    from {{ source('silver', 'silver_trips') }}
    where pickup_date is not null
)

select
    cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_key,
    calendar_date,
    year(calendar_date) as calendar_year,
    month(calendar_date) as calendar_month,
    day(calendar_date) as day_of_month,
    dayofweek(calendar_date) as day_of_week,
    date_format(calendar_date, 'E') as day_name
from dates
