{{ config(materialized='table', file_format='iceberg') }}

-- Grain: exactly one row per validated, deduplicated Silver trip_id.
select
    trip_id,
    operator_code,
    cast(date_format(pickup_date, 'yyyyMMdd') as int) as pickup_date_key,
    pickup_zone_id,
    dropoff_zone_id,
    request_datetime,
    pickup_datetime,
    dropoff_datetime,
    pickup_hour,
    trip_miles,
    trip_time_seconds,
    trip_duration_minutes,
    passenger_fare,
    tolls,
    sales_tax,
    tips,
    driver_pay,
    shared_request_flag,
    shared_match_flag,
    source_year,
    source_month,
    ingestion_run_id
from {{ source('silver', 'silver_trips') }}
