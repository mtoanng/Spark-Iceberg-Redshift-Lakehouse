with identified as (
    select
        *,
        sha2(concat_ws('||', operator_code, cast(pickup_datetime as string), cast(dropoff_datetime as string), cast(pickup_zone_id as string), cast(dropoff_zone_id as string), cast(base_passenger_fare as string), cast(trip_miles as string)), 256) as row_id
    from {{ ref('int_valid_trips') }}
), ranked as (
    select *, row_number() over (partition by row_id order by source_trip_id) as row_rank
    from identified
)
select * except (row_rank)
from ranked
where row_rank = 1
