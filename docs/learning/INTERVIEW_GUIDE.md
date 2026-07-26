# Interview and teach-back guide

This file helps you prove that you understand the system rather than only
recognize its file names. Read the technical guides first:

- [general architecture](README.md);
- [component map](COMPONENT_MAP.md);
- [code-module reference](CODE_MODULE_REFERENCE.md);
- [deployment walkthrough](DEPLOYMENT_WALKTHROUGH.md).

## 1. Thirty-second explanation

This is a replayable monthly NYC TLC HVFHV lakehouse. It pins each source month
by S3 URI, SHA-256, byte size, year, and month. Airflow 3 coordinates Glue 4.0
Bronze, a structural Great Expectations gate, Silver plus reason-coded
quarantine, dbt-glue Gold, count reconciliation, snapshot-aware publication,
and a bounded read-only Athena smoke. Iceberg on S3 is canonical data, Glue
Data Catalog is canonical metadata, and every served month has durable JSON
evidence before it is marked published.

## 2. Ninety-second explanation

The business problem is to produce trustworthy analytical data from official
monthly high-volume ride-hailing records without silently replacing source
history or losing bad rows.

The source Parquet and Taxi Zone CSV are downloaded, hashed, and landed
immutably in one protected S3 bucket. Airflow runs one manual month at a time.
Bronze preserves source fields and adds ingestion plus policy-versioned
identity metadata. `row_id` is the exact-row SHA-256 and the only deduplication
and dbt merge key. `business_trip_key` is a probable-trip investigation key and
never removes data.

A Great Expectations Glue job checks only structural promotability: required
year-specific fields, identity inputs, and a non-empty month. Silver then owns
row-level validation, exact deduplication, derived fields, and deterministic
quarantine reasons. It must classify every Bronze row exactly once.

dbt-glue produces exactly three dimensions, one fact, and two marts.
Reconciliation proves Bronze equals Silver plus quarantine and the monthly
fact equals Silver. Publication then records source identity, layer counts and
snapshots, all six Gold table locations/counts/snapshots, and retained dbt
results in durable JSON before changing the run to published. Athena performs
only parameterized, scan-bounded read queries.

The local contracts can be tested without credentials, but deployment, Glue,
Airflow, Iceberg snapshots, Athena, retries, 2025 evolution, cost, and teardown
remain **NOT VERIFIED** until retained AWS evidence exists.

## 3. Five-minute walkthrough

### Source and identity

One month is not identified only by `2024-01`. It is the combination of source
URI, SHA-256, byte size, year, and month. The upload script refuses to replace
an object with different evidence. Bronze calls S3 `HeadObject` and verifies
the metadata again before Spark reads it. The stable run ID is derived from
the same immutable identity.

Each row receives two hashes from the only policy module.

- `row_id` includes the versioned ordered canonical source fields and controls
  exact deduplication and the Gold merge.
- `business_trip_key` includes a smaller set of likely trip-identifying fields
  and is for analysis only.

The 2025 policy appends `cbd_congestion_fee`; the 2024 policy does not invent
that value.

### Bronze and the structural gate

Bronze reads one landed Parquet object, validates required columns, adds source
and ingestion metadata plus the three identity fields, replaces only the
requested Iceberg partitions, captures a snapshot, and moves the operational
manifest to `bronze_published`.

Great Expectations reads the exact Bronze month/run and checks structural
safety. A missing required field or empty batch becomes `ge_blocked`; Silver
cannot run. A negative fare does not block GX because row validity belongs to
Silver.

### Silver and quarantine

Silver requires the passed gate, resolves Taxi Zones, checks timestamps and
numerics in a fixed first-reason order, detects exact `row_id` duplicates, and
classifies each row.

Valid rows become typed canonical `silver_trips` with date, hour, and duration
derivations. Invalid or duplicate rows retain source and identity evidence in
`quarantine_trips` with one reason code. The task checks:

```text
Bronze = Silver + quarantine
```

before replacing month partitions and saving both snapshots.

### Gold, reconciliation, and publication

dbt builds exactly six Iceberg models. `fct_trips` merges the requested month
using `row_id`; dimensions and marts rebuild over bounded canonical data.
dbt tests uniqueness, not-null fields, relationships, and Silver/fact
reconciliation. Its `run_results.json` is copied to durable S3.

The Glue reconciliation job independently recounts the actual month. It moves
the run to `reconciled` only when every count difference is zero.

Publication requires that state, resolves all required Iceberg metadata,
validates the retained dbt results, writes canonical JSON to S3 with SHA-256
metadata, and only then changes the operational record to `published`.

### Serving and operations

Athena is read-only and limited to four SQL artifacts. The smoke verifies the
exact Gold catalog, non-empty unique monthly fact rows, timestamp bounds, and
the scan-byte limit.

Automatic retry, manual clear, and forced rerun reuse immutable source
identity. Changed URI/hash/size is blocked even with force. A companion DAG
triggers exactly four monthly runs sequentially only after the single-month
baseline is proven.

## 4. Key decisions and their trade-offs

### Why source URI, hash, and size?

A URI alone can point to changed bytes. A checksum identifies content. Byte
size provides an independent transfer/object check and makes mismatch
diagnosis clearer. Year/month binds the physical object to the logical
partition.

Trade-off: correcting a source month needs an explicit reviewed workflow,
because normal force cannot accept changed identity.

### Why hash the exact row?

The source does not provide a reliable canonical trip ID. A policy-versioned
hash makes exact duplicates deterministic across Python, Spark, retry, and dbt.

Trade-off: any identity field or representation change requires a new policy,
golden vectors, schema understanding, and migration reasoning.

### Why retain `business_trip_key`?

Two rows may describe the same probable trip but contain a corrected monetary
or flag value. Keeping both exact rows preserves source truth while the
probable key supports investigation.

Trade-off: analysts must understand that multiple exact rows can share the
business key.

### Why keep GX structural?

Batch structure must block unsafe promotion, but row-level invalidity should
remain measurable. If GX duplicated all Silver rules, ownership and reason
priority could drift and bad rows might disappear behind a task failure.

Trade-off: there are multiple quality components, so their distinct purposes
must be explained clearly.

### Why quarantine rather than drop?

Quarantine preserves evidence, supports reason distributions, and makes
Bronze reconciliation possible.

Trade-off: canonical storage includes an additional table and operational
review path.

### Why month partition replacement?

The project’s replay unit is one immutable month. Replacing only requested
partitions makes retry bounded and prevents unrelated history from being
rewritten.

Trade-off: snapshot IDs can change during replay and must not be confused with
stable row identity.

### Why dbt only for Gold?

Spark owns source-scale ingestion/classification; dbt expresses analytical
model dependencies, grains, and tests clearly.

Trade-off: the runner must support dbt-glue sessions and retain their result
artifacts.

### Why publish after reconciliation?

Successful writes do not prove cross-layer agreement. Publication is the
commit point for served evidence, so it must follow an independent count
check.

Trade-off: data tables can exist before the run is officially published.

### Why write JSON before state?

If state were updated first and the S3 write failed, the system could claim a
publication that cannot be audited. Writing evidence first makes a published
state point to an existing artifact.

Trade-off: a retry may find an artifact from a previous attempt. The object key
is stable and serialization is deterministic for a given document, while
attempt-specific fields such as publication time must be reviewed explicitly.

### Why temporary EC2 Airflow?

The baseline retains a simple controllable runner using an instance profile.
It avoids expanding into a managed orchestration/platform migration before the
data semantics are proven.

Trade-off: installation, patching, scheduler operation, and teardown must be
handled for the temporary runner.

## 5. State-machine questions

### What is the successful state sequence?

```text
discovered
-> bronze_published
-> ge_passed
-> silver_published
-> reconciled
-> published
```

### When can `ge_blocked` occur?

Only after Bronze publication when the requested month is empty or missing
required source/identity columns.

### When may Silver retry after a failure?

The remote job accepts a `failed` row only when `failure_stage='silver'` and
`validation_status='passed'`. This retains the successful structural gate
evidence.

### What makes a run publishable?

Reconciled operational state, complete Bronze/Silver/quarantine counts and
snapshots, exactly six Gold tables with locations/counts/snapshots, and
successful retained dbt invocation/results.

## 6. Record-level scenarios

### Corrected fare, same probable trip

Two rows have equal operator/timestamps/zones but different passenger fare.

- same `business_trip_key`;
- different `row_id`;
- neither is deduplicated solely because the business key matches;
- both may reach Silver if otherwise valid.

### Exact duplicate in one file

Two rows match on every ordered identity field.

- same `row_id`;
- deterministic first row may enter Silver;
- later row enters quarantine as `DUPLICATE_ROW_ID`;
- both Bronze rows remain explained.

### Bad timeline and unknown zone

A row has pickup before request and an unknown pickup zone.

- first matching reason is `PICKUP_BEFORE_REQUEST`;
- zone reason does not replace it;
- deterministic priority makes reason distribution stable on rerun.

### Negative fare in an otherwise valid batch

- GX passes because required columns and non-empty structure exist;
- Silver quarantines the row with `NEGATIVE_PASSENGER_FARE`;
- the batch can still publish if all counts and remaining Gold rows reconcile.

### Empty source month

- Bronze may write/count zero depending on runtime behavior;
- GX non-empty check blocks;
- no Silver, Gold, reconciliation, or publication should complete.

## 7. Operational scenarios

### Same completed source, no force

Bronze detects the same URI/hash/size in a canonical-complete state and skips
canonical Bronze writes. The workflow must not create duplicate fact rows.

### Same completed source, force

The workflow intentionally replays the same immutable month. Month partitions
are replaced and fact merges by `row_id`. Stable evidence should preserve
source identity, run ID, counts, row IDs, and quarantine reasons.

### Changed checksum with force

It is still blocked. Force means “retry these same bytes,” not “replace
history.”

### dbt succeeds but reconciliation fails

Gold tables may physically contain results, but the operational state is not
reconciled or published. Athena serving evidence is incomplete. Diagnose count
scope and canonical keys; do not mark publication manually.

### JSON write succeeds but state update fails

The durable artifact may exist while Ops remains reconciled. A controlled
retry should validate the existing artifact, rebuild canonical evidence, and
then perform the guarded state update. Do not assume artifact existence alone
means published.

### Athena smoke is disabled

The final Bash task prints a deferred message. Publication may exist, but
Athena behavior remains **NOT VERIFIED**.

## 8. Deployment questions

### What runs where?

| Work | Location |
| --- | --- |
| local contracts/tests/package/Terraform plan | developer/CI machine |
| Airflow scheduler/operators/dbt CLI | temporary EC2 runner/container |
| Bronze/GX/Silver/reconciliation/publication | Glue 4.0 jobs |
| dbt Spark execution | dbt-glue session |
| canonical data/metadata | Iceberg S3 + Glue Catalog |
| serving query | Athena workgroup |

### Where do credentials come from?

The AWS default credential chain backed by the EC2 instance profile. Static
access-key environment variables are forbidden by the runner bootstrap and
repository policy.

### What is retained during bounded teardown?

The protected bucket, Glue databases, canonical Iceberg data/metadata, and
publication evidence. Compute/query/IAM resources are the bounded removal
targets.

## 9. Code navigation questions

### Where would you change exact identity?

Only start in `etl/contracts/nyc_hvfhs_identity.py`, then update golden vectors,
Spark parity, table/dbt contracts if schema changes, and documentation.

### Where would you add a new quarantine rule?

`etl/contracts/nyc_hvfhs_quality.py`, in the intended priority position, then
update pure and Spark-facing tests.

### Where would you change the monthly task order?

The Airflow DAG plus exact topology tests and deployment/learning docs.

### Where would you add a seventh Gold model?

You would not under the locked blueprint. Gold is exactly six models.

### Where would you add Iceberg compaction?

You would not in this baseline. It is explicitly out of scope.

## 10. Honest verification language

### Safe claims after local tests

- credential-independent contracts pass;
- Python/Spark identity vectors agree locally;
- DAG topology imports under stubs;
- dbt graph parses/compiles at the CI target;
- Terraform formats/initializes/validates;
- Glue package is deterministic;
- SQL is statically bounded and read-only.

### Prohibited claims without retained AWS evidence

- deployed or production-ready;
- Glue/Airflow/Athena verified;
- exactly-once;
- physical retry/rerun proven;
- snapshot-aware publication executed;
- 2025 evolution or time travel executed;
- scan cutoff enforced in the account;
- cost/performance verified;
- teardown completed.

## 11. Whiteboard exercise

Draw these four lanes:

```text
Airflow | Glue/dbt | Iceberg + Glue Catalog | S3 evidence/Athena
```

Place all eight monthly tasks. For each arrow, label:

- the state required before it starts;
- the argument or artifact passed;
- the table/manifest it reads;
- the output it writes;
- the failure that prevents the next arrow.

Your diagram is incomplete if it omits the dbt results S3 URI or writes
`published` before the publication JSON.

## 12. Debugging exercise

Suppose Ops says:

```text
bronze_row_count=1,000
silver_row_count=970
quarantine_row_count=30
gold_row_count=969
run_status=silver_published
```

Explain:

1. classification is reconciled because `1,000 = 970 + 30`;
2. Gold differs from Silver by one;
3. reconciliation must fail with `gold_vs_silver=-1`;
4. publication must not run;
5. debugging should inspect dbt model/test results and the missing `row_id`,
   not change Bronze counts or manually mark state.

## 13. Learning task

Record yourself giving the five-minute walkthrough without file names. Then
repeat it while naming the exact owner module for identity, row reasons, table
DDL, orchestration, Gold grain, publication JSON, and Athena verification.
Compare the two explanations: the first should show architecture understanding;
the second should show code ownership.

## Teach-back questions

1. A source row changes only `tips`. What happens to `row_id`,
   `business_trip_key`, Silver classification, and the fact merge?
2. Why can a run have physically written Gold tables but still be correctly
   considered unpublished?
3. During a forced identical rerun, which evidence must remain stable, which
   snapshot facts may change, and what retained experiment would prove safety?
