-- Problem: make the canonical trip fact and enrich both zone roles.
-- TODO: cover the two left joins and rebuild them.
with trips as (select * from {{ ref('int_deduplicated_trips') }}),
pickup_zones as (select zone_id, zone as pickup_zone from {{ ref('stg_taxi_zones') }}),
dropoff_zones as (select zone_id, zone as dropoff_zone from {{ ref('stg_taxi_zones') }})
select trips.row_id, trips.source_trip_id, trips.operator_code, trips.pickup_datetime, trips.dropoff_datetime,
    trips.pickup_zone_id, trips.dropoff_zone_id, pickup_zones.pickup_zone, dropoff_zones.dropoff_zone,
    trips.base_passenger_fare, trips.trip_miles, hour(trips.pickup_datetime) as pickup_hour
from trips left join pickup_zones on trips.pickup_zone_id = pickup_zones.zone_id
left join dropoff_zones on trips.dropoff_zone_id = dropoff_zones.zone_id
