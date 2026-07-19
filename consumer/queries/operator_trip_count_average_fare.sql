SELECT
    operator_code,
    trip_count,
    average_passenger_fare
FROM gold.mart_operator_metrics
WHERE source_year = $source_year
  AND source_month = $source_month
ORDER BY trip_count DESC, operator_code;
