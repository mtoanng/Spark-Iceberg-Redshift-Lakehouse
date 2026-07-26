select
    pickup_hour,
    pickup_zone_id,
    count(*) as trip_count,
    cast(sum(base_passenger_fare) as decimal(12, 2)) as total_passenger_fare
from {{ ref('fct_trips') }}
group by pickup_hour, pickup_zone_id
