-- Problem: aggregate canonical fact rows into hourly pickup-zone demand.
-- TODO: cover the aggregation and recreate it from the problem statement.
select pickup_hour, pickup_zone_id, count(*) as trip_count,
    cast(sum(base_passenger_fare) as decimal(12, 2)) as total_passenger_fare
from {{ ref('fct_trips') }}
group by pickup_hour, pickup_zone_id
