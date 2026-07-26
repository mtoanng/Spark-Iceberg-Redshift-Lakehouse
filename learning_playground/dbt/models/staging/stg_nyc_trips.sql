-- Problem: cast the Databricks Bronze source into a clean staging relation.
-- TODO: cover the casts below, then recreate them without looking.
select
    cast(source_trip_id as string) as source_trip_id,
    nullif(trim(hvfhs_license_num), '') as operator_code,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(PULocationID as int) as pickup_zone_id,
    cast(DOLocationID as int) as dropoff_zone_id,
    cast(base_passenger_fare as decimal(10, 2)) as base_passenger_fare,
    cast(trip_miles as decimal(10, 2)) as trip_miles
from {{ source('bronze', 'bronze_nyc_trips') }}
