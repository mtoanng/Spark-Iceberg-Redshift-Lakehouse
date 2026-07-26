# Data semantics

## Identity

Policy `nyc-hvfhv-row-v1-2024` orders 16 canonical fields. Policy
`nyc-hvfhv-row-v1-2025` appends `cbd_congestion_fee`. SHA-256 input starts with
the policy and uses ASCII unit separator between values. Null is `<NULL>`;
timestamps are six-digit ISO form; integers have no decimal point; numeric
measures have six decimal places.

`row_id` means exact source row. `business_trip_key` means probable real-world
trip based on operator/base, request/pickup/drop-off times, and zones.

## Silver reason priority

1. missing/invalid timestamp;
2. pickup before request;
3. drop-off before pickup;
4. invalid zone ID;
5. unknown pickup zone;
6. unknown drop-off zone;
7. missing/invalid numeric;
8. negative miles, duration, fare, tolls, tax, tips, or driver pay in declared order;
9. duplicate `row_id`.

The first applicable reason wins. Every Bronze row reaches either canonical
Silver or quarantine, never both and never neither.

## Retry and publication

One immutable source object has one stable run ID. Same-source retry and
`force=true` replace the same month. Different checksum/URI/size is blocked.
Publication is retry-safe because its S3 key is deterministic by year, month,
and run ID; the artifact is written before the database state becomes
published.
