# Latest controlled-run evidence

No AWS execution evidence is retained yet.

Expected non-sensitive evidence after the first approved run:

```text
source-identity.json
airflow-task-states.json
emr-job-runs.json
layer-counts-snapshots-and-reasons.json
dbt-results.json
reconciliation.json
publication-manifest.json
verification.json
retry-clear-rerun-comparison.json
teardown-verification.json
```

`DEPLOYMENT-VERIFIED` remains **NOT VERIFIED** until these artifacts come from
a real bounded run.
