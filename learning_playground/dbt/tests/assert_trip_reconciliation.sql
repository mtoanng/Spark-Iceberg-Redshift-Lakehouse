-- A singular test returns rows only when the pipeline is inconsistent.
with input_rows as (select count(*) as input_count from {{ ref('stg_nyc_trips') }}),
valid_rows as (select count(*) as valid_count from {{ ref('int_valid_trips') }}),
canonical_rows as (select count(*) as canonical_count from {{ ref('int_deduplicated_trips') }}),
fact_rows as (select count(*) as fact_count from {{ ref('fct_trips') }}),
counts as (
    select input_count, valid_count, input_count - valid_count as quarantine_count,
           canonical_count, fact_count
    from input_rows cross join valid_rows cross join canonical_rows cross join fact_rows
)
select * from counts
where input_count <> valid_count + quarantine_count
   or canonical_count <> fact_count
