SELECT
    trips.pickup_zone_id,
    zone.borough,
    zone.zone_name,
    COUNT(*) AS trip_count
FROM gold.fct_trips AS trips
INNER JOIN gold.dim_zone AS zone
    ON trips.pickup_zone_id = zone.zone_id
WHERE trips.source_year = $source_year
  AND trips.source_month = $source_month
GROUP BY trips.pickup_zone_id, zone.borough, zone.zone_name
ORDER BY trip_count DESC, trips.pickup_zone_id
LIMIT $limit;
