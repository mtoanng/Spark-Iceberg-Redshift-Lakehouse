with trips as (select * from {{ ref('stg_nyc_trips') }}),
zones as (select zone_id from {{ ref('stg_taxi_zones') }}),
checked as (
    select
        trips.*,
        case
            when operator_code is null then 'MISSING_OPERATOR'
            when pickup_datetime is null or dropoff_datetime is null or dropoff_datetime <= pickup_datetime then 'INVALID_TIMESTAMP'
            when base_passenger_fare < 0 then 'NEGATIVE_FARE'
            when zones.zone_id is null then 'UNKNOWN_PICKUP_ZONE'
        end as reason_code
    from trips
    left join zones on trips.pickup_zone_id = zones.zone_id
)
select * except (reason_code)
from checked
where reason_code is null
