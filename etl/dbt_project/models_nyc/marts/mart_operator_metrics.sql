{{ config(materialized='table', file_format='iceberg') }}

-- Grain: one row per source year, source month, and operator.
select
    concat_ws(
        '-',
        cast(source_year as string),
        lpad(cast(source_month as string), 2, '0'),
        operator_code
    ) as operator_month_key,
    source_year,
    source_month,
    operator_code,
    count(*) as trip_count,
    avg(passenger_fare) as average_passenger_fare,
    sum(passenger_fare) as total_passenger_fare,
    sum(driver_pay) as total_driver_pay,
    sum(tips) as total_tips,
    avg(trip_miles) as average_trip_miles
from {{ ref('fct_trips') }}
group by source_year, source_month, operator_code
