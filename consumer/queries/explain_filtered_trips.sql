EXPLAIN ANALYZE
SELECT trip_id, operator_code, pickup_datetime
FROM gold.fct_trips
WHERE source_year = $source_year
  AND source_month = $source_month;
