# Latest controlled-run evidence

No AWS execution evidence is retained yet.

Expected files after the first approved run:

```text
source-manifest.json
airflow-task-states.json
great-expectations-summary.json
layer-counts-and-snapshots.json
dbt-results.json
publication-manifest.json
athena-smoke.json
retry-clear-rerun-comparison.json
teardown-verification.json
```

`CODEBASE-READY` is assessed by local/static checks. `DEPLOYMENT-VERIFIED`
remains **NOT VERIFIED** until these artifacts come from a real bounded run.
