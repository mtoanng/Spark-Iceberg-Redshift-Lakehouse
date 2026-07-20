SELECT
    pickup_date_key,
    pickup_hour,
    pickup_zone_id,
    trip_count
FROM "gold"."mart_hourly_zone_demand"
WHERE pickup_date_key = ?
ORDER BY trip_count DESC, pickup_hour, pickup_zone_id
LIMIT 25;
