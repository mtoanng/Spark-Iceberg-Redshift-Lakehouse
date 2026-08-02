{{ config(materialized='table') }}

select distinct
    operator_code
from {{ source('silver', 'silver_trips') }}
where operator_code is not null
