SELECT
    count(*) AS row_count,
    count(DISTINCT trip_id) AS distinct_trip_count,
    min(pickup_datetime) AS min_pickup_datetime,
    max(dropoff_datetime) AS max_dropoff_datetime
FROM "gold"."fct_trips"
WHERE source_year = ? AND source_month = ?;
