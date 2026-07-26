{{ config(materialized='table', file_format='iceberg') }}

-- Grain: one row per source month, pickup date, pickup hour, and pickup zone.
select
    concat_ws(
        '-',
        cast(source_year as string),
        lpad(cast(source_month as string), 2, '0'),
        cast(pickup_date_key as string),
        lpad(cast(pickup_hour as string), 2, '0'),
        cast(pickup_zone_id as string)
    ) as hourly_zone_key,
    source_year,
    source_month,
    pickup_date_key,
    pickup_hour,
    pickup_zone_id,
    count(*) as trip_count,
    sum(trip_miles) as total_trip_miles,
    avg(passenger_fare) as average_passenger_fare,
    sum(driver_pay) as total_driver_pay
from {{ ref('fct_trips') }}
group by source_year, source_month, pickup_date_key, pickup_hour, pickup_zone_id
