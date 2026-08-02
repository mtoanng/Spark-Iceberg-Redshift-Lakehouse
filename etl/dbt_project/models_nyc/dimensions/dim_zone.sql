{{ config(materialized='table') }}

select
    cast(LocationID as int) as zone_id,
    Borough as borough,
    Zone as zone_name,
    service_zone
from {{ source('bronze', 'bronze_taxi_zones') }}
where LocationID is not null
