# Expected results

Both engines use the same rules and should agree on these values.

| Check | Expected |
| --- | ---: |
| Input rows | 20 |
| Valid rows before deduplication | 16 |
| Quarantined rows | 4 |
| Canonical fact rows | 15 |
| Hourly-zone mart rows | 14 |
| Canonical fact total equals mart trip total | 15 |

Quarantine rows, in priority order:

| source_trip_id | reason_code |
| --- | --- |
| 17 | INVALID_TIMESTAMP |
| 18 | NEGATIVE_FARE |
| 19 | UNKNOWN_PICKUP_ZONE |
| 20 | MISSING_OPERATOR |

The duplicate is the second copy of `source_trip_id=1`; canonical output retains it once. `source_trip_id=14` is valid because pickup zone `1` exists, but its drop-off zone `999` remains unmatched after the left join. The hourly mart row for pickup hour `8`, pickup zone `1` is `trip_count=2` and `total_passenger_fare=22.00`.

The exact row-level result files are:

- `expected/quarantined_trips.csv`
- `expected/canonical_fact_rows.csv`
- `expected/hourly_zone_demand.csv`
