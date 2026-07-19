SELECT
    source_year,
    source_month,
    COUNT(*) AS trip_count,
    SUM(passenger_fare) AS passenger_fare_total,
    SUM(driver_pay) AS driver_pay_total,
    SUM(passenger_fare) - SUM(driver_pay) AS fare_less_driver_pay
FROM gold.fct_trips
WHERE source_year = $source_year
  AND source_month = $source_month
GROUP BY source_year, source_month;
