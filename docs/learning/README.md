# Learning task

Trace one fixture row through the same contracts as the cloud pipeline:

1. calculate its policy-versioned `row_id` and `business_trip_key`;
2. identify the first applicable Silver reason code;
3. explain which layer owns that decision;
4. reconcile the five fixture rows into one Silver and four quarantine rows;
5. map the surviving row to `fct_trips` and both marts;
6. draft the source/count/snapshot fields that publication must contain.

Run:

```powershell
venv\Scripts\python.exe -m pytest `
  tests/unit/test_nyc_identity_contract.py `
  tests/unit/test_nyc_hvfhs_transform.py `
  tests/unit/test_publication_and_rerun.py -v
```

Teach-back:

1. Why can `business_trip_key` support analysis but not exact deduplication?
2. Why does publication consume snapshot IDs from reconciliation instead of
   asking Iceberg for the latest snapshot again?
3. On an identical completed rerun, which artifacts must remain stable and
   which attempt metadata may change?
