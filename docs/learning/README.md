# NYC HVFHV lakehouse learning guide

This directory is the comprehensive guide to how this repository works. It is
organized in three levels so that you can move from the system idea to the
implementation without treating individual files as isolated scripts.

1. **General architecture layer** — this file explains the problem, deployed
   topology, data and control flow, state, failure boundaries, reruns, and the
   2025 snapshot exercise.
2. **Component layer** — [COMPONENT_MAP.md](COMPONENT_MAP.md) explains each
   deployable or logical component, its inputs, outputs, dependencies,
   configuration, permissions, tests, and failure behavior.
3. **Code-module layer** —
   [CODE_MODULE_REFERENCE.md](CODE_MODULE_REFERENCE.md) traces the active
   Python modules, Glue entrypoints, Airflow DAG, dbt models, Athena code,
   deployment scripts, Terraform files, and tests.

Use [DEPLOYMENT_WALKTHROUGH.md](DEPLOYMENT_WALKTHROUGH.md) after the three
layers to follow the exact path from an empty AWS account boundary to a
published month. Use [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) to test whether
you can explain the design and reason through failures.

The architectural source of truth is
[PROJECT2_BLUEPRINT_FINAL.md](../PROJECT2_BLUEPRINT_FINAL.md). If a learning
document and that blueprint disagree, the blueprint wins.

## 1. The system in one sentence

The project takes one checksum-pinned official NYC TLC HVFHV monthly Parquet
file and Taxi Zone lookup, lands them immutably in S3, orchestrates
Bronze-to-Gold Iceberg processing with Airflow 3 and EMR Serverless, classifies every
Bronze row into either canonical Silver or reason-coded quarantine, publishes
snapshot-aware evidence only after dbt and reconciliation succeed, and exposes
only bounded read-only Athena queries.

```text
official immutable HVFHV month + Taxi Zones
        |
        v
S3 landing/reference
        |
        v
Airflow 3 monthly DAG on temporary EC2 runner
        |
        +--> Glue Bronze --> Iceberg bronze + operational manifest
        |
        +--> structural Great Expectations gate
        |
        +--> Glue Silver --> Iceberg silver + quarantine
        |
        +--> dbt-glue --> 3 dimensions + 1 fact + 2 marts
        |
        +--> Glue reconciliation
        |
        +--> Glue publication --> durable JSON evidence
        |
        +--> bounded, read-only Athena smoke
```

Glue Data Catalog is the canonical metadata catalog. Iceberg tables and their
metadata on S3 are canonical data. Airflow coordinates work but is not a data
store. Athena consumes published Gold state but is neither canonical storage
nor a transformation engine.

## 2. Why the architecture is deliberately bounded

The goal is to prove a small production-shaped lakehouse, not to build a
generic platform. The first controlled release is one immutable 2024 month.
Only after that month succeeds does the existing four-month companion DAG run
four consecutive monthly DAGs.

The baseline deliberately does not include MWAA, EKS, Lake Formation,
Glue 5.1, another query engine, dashboards, ML/AI, or an Iceberg maintenance
suite. The optional temporary EC2 runner with an instance profile remains the
Airflow deployment model. This boundary keeps cost, permissions, recovery,
and evidence understandable.

The only post-baseline Iceberg semantic is:

1. add nullable `cbd_congestion_fee` to Bronze, Silver, and quarantine;
2. ingest a 2025 month and retain its new snapshots;
3. verify the current tables read both years;
4. query the retained 2024 snapshot with Athena version travel.

No compaction, snapshot expiration, orphan deletion, partition evolution, or
lifecycle automation belongs to this baseline.

## 3. The four cooperating planes

### 3.1 Data plane

The data plane carries trip rows and Taxi Zone reference rows:

```text
S3 source objects
  -> Bronze Iceberg rows
  -> Silver or quarantine Iceberg rows
  -> Gold Iceberg fact/dimensions/marts
  -> Athena result objects
```

Bronze is source-faithful plus ingestion and identity metadata. Silver owns
types, derived fields, row validation, exact deduplication, and quarantine.
Gold owns analytical grains and aggregations.

### 3.2 Control plane

The control plane is the Airflow DAG:

```text
prepare_month
-> bronze_ingestion
-> great_expectations_checkpoint
-> silver_transform
-> Cosmos `dbt_build` task group
-> dbt_result_artifact
-> reconciliation
-> publication_manifest
-> athena_smoke
```

The DAG is manual, accepts `year`, `month`, and `force`, permits only one active
monthly run, and gives tasks two Airflow retries by default. EMR Serverless jobs perform
distributed work; the DAG passes arguments and waits for completion.

### 3.3 Metadata and state plane

This plane answers “what does the table mean?” and “what state is this monthly
run in?”

- Glue Data Catalog stores namespaces and table definitions.
- Iceberg metadata stores snapshots, schema, partitions, and table history.
- `ops.source_run_manifest` stores immutable source identity, stable run ID,
  identity policy, stage status, counts, snapshots, validation state, failures,
  publication state, and durable artifact URI.
- Airflow XCom carries the current run audit between tasks. It is transient
  orchestration state, not publication evidence.

### 3.4 Evidence plane

The evidence plane makes a successful run auditable after Airflow logs expire:

- uploaded source objects retain `sha256` S3 object metadata;
- the operational manifest retains counts, snapshots, and lifecycle state;
- dbt `run_results.json` is copied to durable S3 storage;
- publication writes canonically serialized JSON containing source identity, layer
  counts/snapshots, every Gold table location/count/snapshot, and dbt summary;
- Athena reports query ID, database, workgroup, result location, state,
  scanned bytes, and engine time.

Publication writes the JSON object before changing the operational manifest to
`published`. A state row therefore must not claim publication when no durable
artifact exists.

## 4. Storage layout and table ownership

Terraform creates one protected, versioned, private, AES-256 encrypted project
bucket. Its logical prefixes are:

```text
s3://<bucket>/
  landing/              immutable fhvhv_tripdata_YYYY-MM.parquet
  reference/            immutable taxi_zone_lookup.csv
  warehouse/            canonical Iceberg data and metadata
    bronze/
    silver/
    ops/
    gold/
  spark_jobs/            EMR Serverless PySpark entry scripts and shared Python zip
  manifests/             dbt results and publication JSON
  athena-results/        Athena output only
  tmp/                   Glue temporary files
```

The bucket has `force_destroy = false`. A bounded teardown intentionally
removes compute/query/IAM resources but retains the bucket, Glue databases,
and canonical data until a separately reviewed data-deletion decision.

### 4.1 Bronze namespace

`bronze.bronze_hvfhs_trips`

- Grain: one row per source Parquet row.
- Partition: `_source_year`, `_source_month`.
- Content: declared source columns plus ingestion metadata and the three
  identity fields.
- Write behavior: requested month partitions are replaced with Iceberg
  `overwritePartitions()`.

`bronze.bronze_taxi_zones`

- Grain: source Taxi Zone lookup rows for the requested run/month.
- Partition: `_source_year`, `_source_month`.
- Used by Silver zone validation and the Gold zone dimension.

Bronze does not discard invalid business rows and does not perform canonical
deduplication.

### 4.2 Silver namespace

`silver.silver_trips`

- Grain: one valid, canonical row per exact `row_id`.
- Partition: `source_year`, `source_month`.
- Contains typed canonical names and derived date/hour/duration fields.

`silver.quarantine_trips`

- Grain: one rejected Bronze row.
- Partition: `_source_year`, `_source_month`.
- Preserves source and identity metadata plus resolved zone columns and one
  deterministic `reason_code`.

Silver must satisfy:

```text
bronze_count = silver_count + quarantine_count
```

No Bronze row disappears and no row is represented in both outputs.

### 4.3 Ops namespace

`ops.source_run_manifest`

- Grain: the active operational record for a source year/month identity.
- Partition: `source_year`, `source_month`.
- Owns the lifecycle from discovery through publication.

The intended successful state sequence is:

```text
discovered
-> bronze_published
-> ge_passed
-> silver_published
-> reconciled
-> published
```

`ge_blocked` records a structural gate failure. `failed` records a stage and
message for operational failures. A changed source identity is rejected rather
than accepted as a new version of the same month.

### 4.4 Gold namespace

Gold is locked to exactly six Iceberg models:

| Model | Grain | Materialization |
| --- | --- | --- |
| `dim_date` | one pickup calendar date | full table |
| `dim_operator` | one observed operator code | full table |
| `dim_zone` | one Taxi Zone ID | full table |
| `fct_trips` | one canonical Silver `row_id` | incremental Iceberg merge |
| `mart_hourly_zone_demand` | month + pickup date + hour + zone | full table |
| `mart_operator_metrics` | year + month + operator | full table |

Only `fct_trips` is incremental. Its `unique_key` is `row_id`, and its SQL is
filtered to the requested `source_year` and `source_month`. Dimensions and
marts rebuild from canonical sources because the controlled dataset is
bounded and clarity is preferred over a generic incremental framework.

## 5. Identity: the core safety contract

There are three different identities. They solve different problems and must
not be substituted for one another.

### 5.1 Landing identity

One monthly input is identified by:

```text
source URI + SHA-256 + byte size + year + month
```

The local upload script calculates the hash and size, stores the hash as S3
object metadata, and refuses to overwrite an existing object unless both hash
and size match. Bronze performs `HeadObject` and verifies those facts again
before Spark reads either source object.

The stable ingestion run ID is:

```text
fhvhv-<year>-<month>-<first 16 checksum hex characters>
```

The same immutable source therefore receives the same run ID across Airflow
retry, task clear, and deliberate monthly rerun.

### 5.2 Exact-row identity: `row_id`

`row_id` is the canonical deduplication and merge key. It is SHA-256 over:

1. the source-year identity policy version;
2. every ordered identity field for that policy;
3. explicit canonical representations for nulls, timestamps, integers,
   decimals, and text;
4. an unambiguous field separator.

The 2024 field list does not contain `cbd_congestion_fee`. The 2025 list adds
it. Ingestion timestamps and Silver-derived fields never participate.

The only policy source is
[etl/contracts/nyc_hvfhs_identity.py](../../etl/contracts/nyc_hvfhs_identity.py).
Its Python and Spark expressions are tested against the same pinned golden
vectors. This prevents a local fixture and a Glue run from generating
different hashes for the same row.

### 5.3 Probable-trip identity: `business_trip_key`

`business_trip_key` hashes a smaller set of operator, dispatch, time, and zone
fields. It is useful for investigating two source rows that may represent the
same real-world trip.

It is not an exact identity. A corrected fare or tip can preserve
`business_trip_key` while changing `row_id`. Therefore it never:

- deduplicates Silver;
- drives the dbt merge;
- silently removes a row;
- sends a row to quarantine by itself.

## 6. Quality ownership

The project has three quality boundaries because they answer different
questions.

### 6.1 Great Expectations: “is the batch structurally promotable?”

The Great Expectations Glue task runs after Bronze and checks only:

- all year-specific required columns exist;
- all identity-policy input columns exist;
- the requested Bronze month is non-empty.

It persists a concise summary and changes the state to `ge_passed` or
`ge_blocked`. It does not decide whether an individual trip has a negative
fare, invalid zone, or bad timestamp order.

### 6.2 Silver: “where does each row belong?”

Silver applies one deterministic priority:

1. missing/invalid timestamps;
2. pickup before request;
3. drop-off before pickup;
4. invalid zone IDs;
5. unknown pickup zone;
6. unknown drop-off zone;
7. missing/invalid numerics;
8. negative numeric measures in the declared order;
9. duplicate `row_id`.

The first matching reason wins. That priority is shared by pure Python and
Spark through [etl/contracts/nyc_hvfhs_quality.py](../../etl/contracts/nyc_hvfhs_quality.py).
An invalid row remains visible in quarantine with its original identity.

### 6.3 Reconciliation: “did the published layers agree?”

After dbt succeeds, the Glue reconciliation job checks the actual month:

```text
manifest Bronze count = actual Silver count + actual quarantine count
manifest Silver count = actual Silver count
manifest quarantine count = actual quarantine count
Gold fct_trips count = actual Silver count
```

Only a zero difference for every check changes the manifest to `reconciled`
and sets publication to `pending`.

## 7. One monthly run, step by step

### Step 0: initialize tables

The initializer creates the Bronze, Silver, and Ops namespaces/tables defined
in `etl/iceberg/catalog.py`. Gold tables are created by dbt when models run.
For the post-baseline exercise only, `--APPLY_2025_EVOLUTION true` executes the
three approved `ADD COLUMN cbd_congestion_fee DOUBLE` statements.

### Step 1: `prepare_month`

Airflow validates the manual parameters, reads immutable source facts from
Airflow Variables, constructs the landed S3 URI, and creates a `SourceFile`.
`audit_for_source()` verifies that request month and source month agree and
derives the stable run ID. The returned dictionary becomes XCom input for
every later task.

### Step 2: `bronze_ingestion`

The Glue job:

1. validates the S3 URI, year/month, checksum shape, size, and run ID;
2. verifies source and Taxi Zone S3 metadata through `HeadObject`;
3. reads the existing operational manifest and blocks changed identity;
4. skips canonical writes for an already completed identical source unless
   `force=true`;
5. reads Parquet and validates the year-specific source schema;
6. generates `row_id`, `business_trip_key`, and policy version in Spark;
7. appends ingestion metadata without business cleaning;
8. replaces only requested Bronze partitions;
9. captures the current Bronze snapshot ID;
10. writes Taxi Zones and changes the manifest to `bronze_published`.

On failure it records `failure_stage` and a bounded message before re-raising.

### Step 3: `great_expectations_checkpoint`

The Glue job refuses to start unless the same manifest row is
`bronze_published`. It filters Bronze to year, month, and run ID, runs an
ephemeral GX Spark dataframe asset, performs the non-empty check, persists a
JSON summary, and raises if the result is blocked. Airflow therefore cannot
schedule Silver after a failed gate.

### Step 4: `silver_transform`

The Glue job requires `ge_passed`, or a retryable Silver-stage failure whose
validation status remains passed. It:

1. reads only the requested Bronze run;
2. confirms its identity policy matches the year;
3. loads month-scoped Taxi Zone IDs;
4. loads `row_id` values already published outside the requested month;
5. left-joins zone resolution and existing identity evidence;
6. applies the ordered business reason expression;
7. deterministically numbers repeated `row_id` values inside the batch;
8. marks later occurrences or cross-month existing IDs
   `DUPLICATE_ROW_ID`;
9. derives typed Silver columns;
10. calculates counts from one persisted classified frame;
11. replaces requested Silver and quarantine partitions;
12. captures both snapshot IDs and changes state to `silver_published`.

### Step 5: Cosmos `dbt_build` and `dbt_result_artifact`

Cosmos `DbtTaskGroup` runs one Watcher-mode `dbt build` producer against the
Glue target with explicit month variables. The resulting model watchers expose
each model/test outcome in Airflow while dbt:

- builds/tests three dimensions;
- merges the requested month into `fct_trips` by `row_id`;
- rebuilds/tests two marts;
- executes the singular fact-to-Silver reconciliation test.

After a successful full producer run, its Cosmos callback copies
`target/run_results.json` to the deterministic publication S3 prefix. The
separate `dbt_result_artifact` task requires that object to be non-empty and
SHA-256-tagged, then returns its durable URI through XCom.

### Step 6: `reconciliation`

The Glue job recounts actual Silver, quarantine, and monthly fact rows. It
compares them with manifest counts and fails on any difference. Success sets
`run_status='reconciled'`, stores `gold_row_count`, and makes publication
pending.

### Step 7: `publication_manifest`

The Glue job requires `reconciled`. It then:

1. verifies all six Gold tables exist;
2. obtains every Gold table location, row count, and latest snapshot;
3. loads durable dbt results and rejects missing or failed results;
4. requires Bronze, Silver, and quarantine counts and snapshots;
5. builds canonical sorted JSON;
6. writes the month/run-keyed JSON with its own SHA-256 metadata;
7. only then changes the operational row to `published`.

### Step 8: `athena_smoke`

When explicitly enabled, the Airflow runner verifies exactly six Gold catalog
tables and required columns, executes a parameterized month-filtered query,
and requires:

- one result row with four values;
- positive fact count;
- `count(*) = count(distinct row_id)`;
- non-null timestamp bounds;
- scan bytes at or below the configured bound.

The runner returns the audit facts needed for retained evidence. SQL remains
single-statement and read-only.

## 8. Retry, clear, force, and rerun semantics

These operations are related but not identical:

| Operation | What restarts | Source identity rule | Expected canonical result |
| --- | --- | --- | --- |
| Airflow automatic retry | failed task attempt | identical XCom/source facts | same month partitions and keys |
| Manual task clear | selected task and downstream work | same DAG-run audit | same counts/keys after recovery |
| Rerun with `force=false` | new monthly DAG run | identical completed source skips Bronze writes | no duplicate canonical rows |
| Rerun with `force=true` | deliberate identical-source replay | changed identity still blocked | replaced month state, same canonical evidence |
| Changed URI/hash/size | candidate monthly run | rejected even with force | existing month remains authoritative |

Month-scoped `overwritePartitions()` makes Bronze, Silver, and quarantine
replayable. `fct_trips` uses an Iceberg merge on `row_id`. The evidence
comparator checks stable identity, counts, row ID sets, and quarantine reasons.

These are code contracts. Physical Glue retry, Airflow clear, Iceberg snapshot
behavior, and scheduler recovery remain **NOT VERIFIED** until a retained AWS
experiment demonstrates them.

## 9. Four-month sequence

The companion DAG does not perform transformations. It builds exactly four
validated requests and triggers the monthly DAG four times, sequentially,
waiting for each month before starting the next. The starting month must be no
later than September so the sequence cannot silently cross a year boundary.

The controlled order is:

```text
prove one 2024 month
-> inspect evidence and failures
-> run months M, M+1, M+2, M+3 sequentially
```

This is intentionally not a generic backfill framework.

## 10. Deployment mental model

Terraform provisions one data boundary and the jobs/roles that operate within
it. The deterministic Glue package is built before Terraform validation
because Terraform hashes and uploads that file. An approved apply creates:

- protected project S3 bucket;
- `bronze`, `silver`, `ops`, and `gold` Glue databases;
- Glue shared package and six job scripts in S3;
- initializer, Bronze, GX, Silver, reconciliation, and publication Glue jobs;
- Glue service role and scoped access policy;
- bounded Athena workgroup;
- Airflow runner IAM role;
- optional EC2 runner and instance profile when AMI/subnet are supplied.

The EC2 instance profile is the AWS credential source. Static keys are
forbidden. The current bootstrap script only prepares directories and checks
that static key variables are absent; the reviewed Airflow image and DAG still
must be installed and started as described in the deployment runbook.

Read [DEPLOYMENT_WALKTHROUGH.md](DEPLOYMENT_WALKTHROUGH.md) for the exact
configuration handoff among Terraform outputs, upload-script output, Airflow
Variables, environment variables, Glue arguments, and S3 evidence.

## 11. Failure boundaries

| Boundary | Failure example | Durable effect | What must not happen |
| --- | --- | --- | --- |
| Local staging | incomplete download | `.part` removed | partial file promoted |
| S3 landing | changed existing object | upload rejected | overwrite immutable month |
| Bronze pre-read | S3 metadata mismatch | manifest failure when possible | Spark reads untrusted object |
| Source manifest | URI/hash/size differs | `source_manifest` failure | force accepts changed content |
| GX gate | missing column or empty month | `ge_blocked` | Silver runs |
| Silver | invalid individual row | quarantine reason | silent row loss |
| Silver job | Spark/write error | `failed`, stage `silver` | validation pass forgotten |
| dbt | model/test failure | task fails; run results not publishable | reconciliation/publication run |
| Reconciliation | count mismatch | task raises | run marked reconciled |
| Publication | missing snapshot/dbt metadata | no published state | published without JSON |
| Athena | wrong schema/count or high scan | smoke fails | write or unbounded query |

## 12. What local tests prove

Credential-independent tests prove:

- source naming, schema, checksum/size, and immutable decisions;
- Python and Spark identity agreement on golden vectors;
- Bronze metadata contract;
- reason priority, deduplication, quarantine, and reconciliation on fixtures;
- operational state transitions and retry decisions;
- Glue job static contracts and DAG topology;
- exact six-model dbt contract and parse/compile;
- publication completeness and deterministic serialization for a given document;
- bounded Athena SQL and runner behavior;
- Terraform formatting/validation and package dependencies;
- source upload idempotence, evidence comparison, and teardown-verifier logic;
- repository hygiene and credential scanning.

They do not prove actual AWS execution.

## 13. Verification boundary

Until a retained controlled AWS run exists, the following remain
**NOT VERIFIED**:

- Terraform apply and real resource creation;
- source upload into the target account;
- EMR Serverless execution and CloudWatch behavior;
- actual Iceberg writes, partitions, and snapshot IDs;
- Great Expectations inside Glue;
- dbt-glue interactive session and model execution;
- Airflow runner installation, scheduling, retry, clear, and rerun;
- reconciliation against real monthly counts;
- publication artifact in S3;
- Athena query ID/results/scan cutoff;
- four-month sequence;
- 2025 schema evolution and 2024 version travel;
- cost, performance, and bounded teardown.

Use `CODEBASE-READY` for a clean credential-independent verification. Use
`DEPLOYMENT-VERIFIED` only after the required AWS evidence is retained.

## 14. Recommended study path

1. Read this file once without opening code.
2. Read [COMPONENT_MAP.md](COMPONENT_MAP.md) and draw the data, control,
   metadata, and evidence planes.
3. Read the identity and quality sections of
   [CODE_MODULE_REFERENCE.md](CODE_MODULE_REFERENCE.md), then inspect those
   source files.
4. Trace one fixture row through pure Bronze and Silver functions.
5. Trace the corresponding remote path through the three Glue jobs.
6. Read the dbt fact and marts and state their exact grains.
7. Follow [DEPLOYMENT_WALKTHROUGH.md](DEPLOYMENT_WALKTHROUGH.md) and explain
   every configuration handoff.
8. Use [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) without looking at answers.

## Learning task

Draw one successful monthly run using four swimlanes: Airflow, Glue/dbt,
Iceberg/Glue Catalog, and S3 evidence. For every task, write its input state,
output state, table or artifact written, and the condition that blocks the
next task.

## Teach-back questions

1. Why are landing identity, `row_id`, and `business_trip_key` three separate
   concepts, and which one controls each rerun, deduplication, and analytical
   investigation decision?
2. How do Great Expectations, Silver quarantine, reconciliation, and
   publication each prevent a different failure from becoming served data?
3. After the Cosmos `dbt_build` group succeeds, what exact evidence must exist before the
   operational manifest may become `published`, and what does Athena verify
   afterward?
