# Dataset Notes — NYC TLC High Volume FHV

## Approved sources

- TLC trip-record page: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- Selected MVP source: <https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet>
- Taxi Zone lookup: <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv>

The selected production month is **2024-01**. The `2025-01` fixture exists
only to express the later schema contract; it is not an approved Phase 1
ingestion target.

## Production-source evidence

| Source | Exact URI | Size | Checksum | Status |
| --- | --- | ---: | --- | --- |
| HVFHV 2024-01 | `fhvhv_tripdata_2024-01.parquet` URI above | NOT VERIFIED | NOT VERIFIED | The source must be downloaded to a controlled staging location and SHA-256 calculated before cloud ingestion. A 2026-07-19 HTTP HEAD request returned 403, so no size or checksum is recorded. |
| Taxi Zone lookup | URI above | 12,331 bytes | ETag `c6064b7c144c716450641f769659d178` | HTTP HEAD verified on 2026-07-19. The ETag is not treated as a SHA-256 checksum; calculate SHA-256 after download. |

No NYC TLC production file is committed to Git.

## Local staging observation (2026-07-19)

Parquet-footer inspection was performed without scanning rows or starting
Spark. The following locally staged files match the declared 2024 source
schema. Their SHA-256 checksums have not yet been calculated, so provenance
against the official objects is still not verified.

| Month | Local file | Size (bytes) | Footer row count | SHA-256 |
| --- | --- | ---: | ---: | --- |
| 2024-01 | `data/fhvhv_tripdata_2024-01.parquet` | 472,757,547 | 19,663,930 | NOT VERIFIED |
| 2024-02 | `data/fhvhv_tripdata_2024-02.parquet` | 462,475,428 | 19,359,148 | NOT VERIFIED |
| 2024-03 | `data/fhvhv_tripdata_2024-03.parquet` | 507,940,560 | 21,280,788 | NOT VERIFIED |

Only these three matching monthly files were present during the Phase 2
inspection. A fourth month was not assumed or processed.

## Schema contract

The source adapter requires the following 2024 source columns:

`hvfhs_license_num`, `dispatching_base_num`, `request_datetime`,
`pickup_datetime`, `dropoff_datetime`, `PULocationID`, `DOLocationID`,
`trip_miles`, `trip_time`, `base_passenger_fare`, `tolls`, `sales_tax`,
`tips`, `driver_pay`, `shared_request_flag`, and `shared_match_flag`.

For 2025 and later, `cbd_congestion_fee` is additionally required. The TLC
trip-record page states that this column was added to High Volume FHV data from
2025 onward. The Phase 1 adapter only detects this shape; schema evolution is
explicitly deferred to Phase 7.

## Deterministic fixtures

`tests/fixtures/nyc_hvfhs/` contains source-shaped, small test data only:

- `fhvhv_tripdata_2024-01.fixture.json`: valid trip, duplicate, invalid time
  order, invalid zone ID, and negative metric.
- `taxi_zone_lookup.fixture.csv`: minimal zone lookup that intentionally omits
  zone `999`.
- `fhvhv_tripdata_2025-01.fixture.json`: one source-shaped record with
  `cbd_congestion_fee`.

| Fixture | Size (bytes) | SHA-256 |
| --- | ---: | --- |
| `fhvhv_tripdata_2024-01.fixture.json` | 2,689 | `77be47c88c1ae823be38bebf3b3c30d3b564b87f05023d270270caabced64b83` |
| `fhvhv_tripdata_2025-01.fixture.json` | 625 | `652ad753e710e14b18f2020f098b93f224892f564537dc1868df6f2295cc9f84` |
| `taxi_zone_lookup.fixture.csv` | 135 | `276b2b2febced718bad06d370397eec5c8a13840c6b3d2daccf28bd08bbd1023` |

The Phase 1 unit test pins this metadata. These fixtures are not substitutes
for production-source evidence.
