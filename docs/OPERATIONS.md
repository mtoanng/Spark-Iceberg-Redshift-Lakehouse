# Operations

Manifest states are:

```text
discovered -> bronze_published -> ge_passed -> silver_published
-> reconciled -> published
```

GE failure becomes `ge_blocked`; task failures record `failed` with stage and
message. Operators diagnose the first failed stage, retain evidence, and retry
only the bounded task. A clear/retry never changes source identity.

Do not manually edit canonical tables. Changed monthly content requires a
separately reviewed replacement workflow. Do not recursively delete canonical
warehouse or landing prefixes during teardown.

Athena execution must report query ID, database, workgroup, state, result
location, scanned bytes, and engine time. Workgroup cutoff is authoritative.

Deployment health is not data correctness. A run is publishable only after
classification reconciliation, Gold reconciliation, six table snapshots, and
durable publication JSON all resolve.
