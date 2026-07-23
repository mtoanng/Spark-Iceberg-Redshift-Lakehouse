# Dataset notes

The only production source is the official NYC TLC High Volume For-Hire
Vehicle monthly Parquet dataset plus the official Taxi Zone lookup:

- `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet`
- `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

The first deployment is 2024-01; the bounded backfill is 2024-01 through
2024-04. No source file is committed. `scripts/fetch_source.py` streams one
month to ignored local staging, and `scripts/upload_release_dataset.py`
computes identity and performs an immutable S3 upload only with `--execute`.

On 2026-07-23 an HTTP request to the January object returned 200 and a
472,757,547-byte content length; the Taxi Zone object returned 200 and 12,331
bytes. These reachability observations are not SHA-256 or AWS landing evidence.

Ignored local staging currently contains four monthly files with sizes
472,757,547; 462,475,428; 507,940,560; and 476,115,643 bytes respectively.
Their official-object identity remains **NOT VERIFIED** until checksums are
computed during the controlled upload workflow.

The required 2024 trip columns are `hvfhs_license_num`,
`dispatching_base_num`, `request_datetime`, `pickup_datetime`,
`dropoff_datetime`, `PULocationID`, `DOLocationID`, `trip_miles`, `trip_time`,
`base_passenger_fare`, `tolls`, `sales_tax`, `tips`, `driver_pay`,
`shared_request_flag`, and `shared_match_flag`. Bronze preserves source fields
and adds ingestion metadata; it does not ingest pre-aggregated or cleaned
copies.

`tests/fixtures/nyc_hvfhs/` contains only small deterministic source-shaped
fixtures. The pinned fixture hashes are contract-tested; they are not proof of
production source provenance. The 2025 fixture is retained solely to keep the
source schema contract forward-readable; schema evolution is not implemented
for the first deployment.
