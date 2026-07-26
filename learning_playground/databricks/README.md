# Databricks Free Edition exercises

Import this repository into a Databricks Git folder (or import each `.py` file as a notebook). Upload the two committed fixture files to a small Unity Catalog Volume you can read, for example `/<your volume>/learning_data/`. In every notebook set only:

```python
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "/Volumes/<learner_catalog>/<schema>/learning_data"
```

If you cannot create a catalog or schema in Free Edition, leave the first two values as labels: all notebooks register temporary views and do not require creating persistent tables. Attach a Serverless notebook and run the solved notebook in `solutions/` first. Then use the same numbered starter notebook and complete its TODO.

| Notebook | One concept | Expected check |
| --- | --- | --- |
| `01_read_and_profile` | DataFrame schema/profile | 20 rows, 1 null operator |
| `02_validate_and_quarantine` | ordered validation | 16 valid, 4 quarantined |
| `03_deduplicate_trips` | hash + `row_number` | 15 canonical rows |
| `04_join_taxi_zones` | two left dimension joins | trip 14 has null drop-off zone |
| `05_hourly_zone_demand` | grouping/aggregation | hour 8, zone 1 = 2 trips, 22.00 fare |
| `06_reconcile_pipeline` | conservation checks | `20 = 16 + 4`, `15 = 15` |

Each solution reads only the two uploaded CSVs, uses PySpark and Spark SQL built into Databricks, and creates temporary views. No AWS SDK, cluster libraries, cloud paths, or production code is used.
