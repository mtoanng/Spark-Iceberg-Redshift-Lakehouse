# Four-month sequential backfill

Status: **requires AWS execution verification**. Start only after the one-month
run, its retry experiment, reconciliation, evidence review, and cost review all
pass.

Upload four consecutive official monthly files with
`scripts/upload_release_dataset.py`; run dry-run before each approved
`--execute`. Set each month-specific SHA-256 and size Airflow variable from the
script output.

Add the SHA-256 and byte-size `AIRFLOW_VAR_*` values for all four months before
starting the same reviewed container. Trigger the parent DAG with:

```bash
docker exec nyc-hvfhs-airflow airflow dags trigger nyc_hvfhs_four_month_backfill \
  --conf '{"year": 2024, "month": 1, "force": false}'
docker exec nyc-hvfhs-airflow airflow dags list-runs \
  --dag-id nyc_hvfhs_four_month_backfill
```

The approved demo must trigger January, February, March, and April sequentially
with `wait_for_completion=true`. The planner also handles calendar-year
rollover correctly and tests October-January, but a cross-year cloud run is
outside the first deployment because 2025 schema evolution is not implemented.

For every child run, capture source identity, counts, GE result, quarantine
reasons, dbt result, reconciliation, publication URI, and Athena scanned bytes.
If one month fails, later months must not start. Correct the cause and rerun the
same monthly request; do not change identity metadata or use `force` to bypass
immutability. Teardown follows the same protected procedure as the one-month
run.
