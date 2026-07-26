-- Problem: give the Databricks Bronze Taxi Zone source stable column names.
-- TODO: cover the select list and write it from memory.
select cast(LocationID as int) as zone_id, zone, borough
from {{ source('bronze', 'bronze_taxi_zones') }}
