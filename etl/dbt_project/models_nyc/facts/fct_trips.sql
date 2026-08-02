{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='row_id'
) }}

-- Grain: exactly one row per validated, deduplicated Silver row_id.
select
    row_id,
    business_trip_key,
    identity_policy_version,
    operator_code,
    cast(to_char(pickup_date, 'YYYYMMDD') as integer) as pickup_date_key,
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
    {% if var('source_year') | int >= 2025 %}
    cbd_congestion_fee,
    {% else %}
    cast(null as double precision) as cbd_congestion_fee,
    {% endif %}
    source_year,
    source_month,
    ingestion_run_id
from {{ source('silver', 'silver_trips') }}
where source_year = {{ var('source_year') | int }}
  and source_month = {{ var('source_month') | int }}
