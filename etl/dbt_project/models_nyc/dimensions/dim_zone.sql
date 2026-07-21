{{ config(materialized='table', file_format='iceberg') }}

select
    cast(LocationID as int) as zone_id,
    max(Borough) as borough,
    max(Zone) as zone_name,
    max(service_zone) as service_zone
from {{ source('bronze', 'bronze_taxi_zones') }}
where LocationID is not null
group by cast(LocationID as int)
