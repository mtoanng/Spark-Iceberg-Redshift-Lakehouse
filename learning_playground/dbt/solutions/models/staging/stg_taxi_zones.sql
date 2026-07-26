select
    cast(LocationID as int) as zone_id,
    zone,
    borough
from {{ source('bronze', 'bronze_taxi_zones') }}
