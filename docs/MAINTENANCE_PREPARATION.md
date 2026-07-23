# Future Iceberg maintenance preparation

Status: **not implemented** and **requires AWS execution verification**.

Do not run maintenance before the one-month deployment, retry experiment, and
evidence review succeed. First confirm the Iceberg version bundled by the
selected Glue runtime supports each procedure and take a metadata inventory.

The future, table-by-table Spark SQL entry points are:

```sql
-- Inventory only: safe orphan candidate listing.
CALL glue_catalog.system.remove_orphan_files(
  table => 'gold.fct_trips',
  dry_run => true
);

-- Destructive: choose and review an explicit UTC cutoff and retention count.
CALL glue_catalog.system.expire_snapshots(
  table => 'gold.fct_trips',
  older_than => TIMESTAMP '<reviewed-utc-cutoff>',
  retain_last => <reviewed-positive-count>
);

-- Mutating compaction: run only after file metrics prove it is needed.
CALL glue_catalog.system.rewrite_data_files(
  table => 'gold.fct_trips'
);
```

Snapshot expiration and compaction have no dry-run contract in this project;
the placeholders intentionally prevent copy-paste execution. Orphan cleanup
must remain `dry_run => true` until its candidate list is independently
reviewed against Iceberg metadata and an explicit deletion approval exists.
Automation, scheduling, and multi-table maintenance are deferred.
