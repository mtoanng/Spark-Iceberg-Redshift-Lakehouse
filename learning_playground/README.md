# NYC lakehouse learning playground

This folder is an intentionally small, local-to-the-repository learning path. It uses 20 inspectable CSV rows and teaches the same core boundaries as the production lakehouse without AWS Glue, Airflow, Iceberg, Terraform, or credentials.

Learning order:

1. Run Databricks `01_read_and_profile`.
2. Complete validation and deterministic deduplication in `02` and `03`.
3. Run the dbt staging models and follow `ref()` through the graph.
4. Run dbt tests and the two marts.
5. Run `06_reconcile_pipeline` and compare it with the dbt singular test.

The tiny fixture has 20 input rows: 16 valid rows before deduplication, four quarantined rows, and 15 canonical facts. It deliberately has one valid trip with an unmatched **drop-off** zone so that the zone-join lesson can retain evidence rather than discard it.

The six starter notebooks and six dbt models include a short `TODO`; working reference answers live in each module's `solutions/` directory. The active dbt models are runnable reference implementations so that `dbt build` works immediately. For a blind exercise, cover their query body and use the matching solution only after trying it.

Use only the committed files in `data/`; do not put real TLC Parquet files or credentials in this folder. Exact expected results are in [EXPECTED_RESULTS.md](EXPECTED_RESULTS.md).
