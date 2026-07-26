# Controlled AWS runbook

## Prepare without AWS execution

1. Configure only role-based credentials/instance profile.
2. Pin official source URI, SHA-256, byte size, and Taxi Zone checksum.
3. Build `build/nyc_glue_jobs.zip`.
4. Run Python, dbt, DAG, Terraform, packaging, hygiene, and secret checks.
5. Review `terraform plan`; do not apply without separate approval.

## One-month baseline

After approved provisioning and official source upload, trigger:

```bash
airflow dags trigger nyc_hvfhs_monthly \
  --conf '{"year":2024,"month":1,"force":false}'
```

Retain manifest state, GE summary, Bronze/Silver/quarantine counts and
snapshots, dbt results, all six Gold counts/snapshots, publication JSON, and
Athena query metadata.

Configure Airflow Variable `nyc_publication_prefix_uri` from Terraform output
`publication_prefix_uri`; the dbt task uploads `target/run_results.json` there
and passes its URI to publication.

## Retry, clear, and rerun experiment

1. Run the month once.
2. Interrupt the controlled Silver task.
3. Let its configured task retry execute.
4. If needed, clear only the failed task:

```bash
airflow tasks clear nyc_hvfhs_monthly silver_transform \
  --start-date <logical-date> --end-date <logical-date> --yes
```

5. Finish the run, then trigger the same month with `"force": true`.
6. Export first/rerun evidence and compare:

```bash
python scripts/verify_monthly_rerun.py first.json rerun.json
python scripts/reconcile_outputs.py rerun.json
```

A different checksum must fail in Bronze manifest guarding.

## 2025 evolution after baseline

Run the existing initializer once with `--APPLY_2025_EVOLUTION true`, ingest
one 2025 source, and retain old/current snapshots. Render the bounded Athena
version-travel template using only validated identifiers and bind the old
snapshot ID as parameter. Verify retained evidence:

```bash
python scripts/verify_schema_evolution.py schema-evolution.json
```

Expected: historical query returns the 2024 count; current query returns 2024
plus 2025; old rows expose nullable congestion fee.

Live AWS execution, snapshots, version travel, retry, and teardown are
**NOT VERIFIED** until those artifacts exist.
