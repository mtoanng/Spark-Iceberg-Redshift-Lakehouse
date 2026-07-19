-- Returns one row only when the Gold fact count differs from valid Silver.
with silver_count as (
    select count(*) as row_count
    from {{ source('silver', 'silver_trips') }}
),
fact_count as (
    select count(*) as row_count
    from {{ ref('fct_trips') }}
)

select
    silver_count.row_count as silver_rows,
    fact_count.row_count as fact_rows
from silver_count
cross join fact_count
where silver_count.row_count <> fact_count.row_count
