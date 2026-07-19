SELECT
    demand.pickup_date_key,
    demand.pickup_hour,
    demand.pickup_zone_id,
    zone.borough,
    zone.zone_name,
    demand.trip_count
FROM gold.mart_hourly_zone_demand AS demand
INNER JOIN gold.dim_zone AS zone
    ON demand.pickup_zone_id = zone.zone_id
ORDER BY demand.pickup_date_key, demand.pickup_hour, demand.trip_count DESC, demand.pickup_zone_id;
