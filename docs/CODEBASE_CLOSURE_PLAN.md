# Project 2 codebase closure plan

Audit date: 2026-07-20

Status: **AUDIT COMPLETE; IMPLEMENTATION AND AWS DEPLOYMENT NOT VERIFIED**

## Frozen target

```text
Official NYC TLC HVFHV Parquet
-> S3 Landing
-> Airflow 3
-> AWS Glue / PySpark
-> Iceberg Bronze
-> Great Expectations
-> Iceberg Silver
-> dbt-glue Gold
-> Amazon Athena
```

AWS Glue Data Catalog is the canonical catalog. S3/Iceberg is canonical data
storage. Athena is the active read-only query surface. The retired local query
engine is not part of
the active architecture.

This target overrides the retired serving-layer and post-Gold equivalent-checkpoint portions
of `AGENTS.md`, `docs/PROJECT2_BLUEPRINT_FINAL.md`, `README.md`, and the older
phase reports for future implementation. The phase reports remain historical
evidence and must not be rewritten to imply that earlier work used Athena.

## Audit conclusion

The NYC source contracts, fixture transformations, Iceberg table DDL, six dbt
Gold models, manual Airflow DAG shape, S3 baseline, Glue IAM baseline, and
credential-independent CI checks are useful foundations. The repository is not
yet deployable end to end from a fresh checkout.

The blocking gaps are:

1. Great Expectations is absent and the current quality task runs after Gold,
   not between Bronze and Silver.
2. Athena has no Terraform, IAM, SQL, runner, verifier, or tests.
3. Terraform does not create an Airflow runner or its instance profile.
4. Glue script packaging is incomplete: uploaded entry points import project
   modules that are not packaged with the jobs.
5. Iceberg catalog configuration and Glue database/namespace naming are not
   consistently wired across Terraform, Glue jobs, and dbt.
6. Bronze and Silver physical writes are append-based and not proven
   idempotent; Silver is not scoped to the requested source month.
7. The source manifest is an in-memory contract, not a durable cloud manifest.
8. Provisioning, source upload, reconciliation, verification, and teardown are
   described manually but do not have current NYC/Athena scripts.
9. Ignored local `.env` contains static AWS credential variable names and
   legacy Instacart/MongoDB/local-query settings. The values were not copied into
   this audit. If they were ever real, they require rotation before any cloud
   work.
10. The saved Terraform state and tracked `terraform/tfplan` describe the old
    Instacart/ML stack, not the active NYC Terraform configuration.

## Closure sequence

Complete these work packages in order. Each package must preserve the prior
credential-independent checks and must not claim AWS verification.

### 1. Reconcile repository governance

- Update `AGENTS.md`, the final Project 2 blueprint, `README.md`, and
  `docs/CODEBASE_INDEX.md` to the frozen architecture.
- State that Great Expectations is the pre-Silver contract gate and that the
  existing post-Gold count check is reconciliation, not the Great Expectations
  checkpoint.
- Mark the retired local query layer and its tests inactive before removing them in a separately
  approved cleanup pass.
- Keep completed phase reports immutable as historical records.

Gate: active architecture documentation contains no retired-serving path and no
claim of verified AWS execution.

### 2. Secure and normalize local deployment inputs

- Add `.env.cloud.example` with names only, no credentials or account IDs.
- Use the AWS default credential provider chain locally and an EC2 instance
  profile on the temporary Airflow runner; never require committed access keys.
- Replace obsolete private tfvars keys with the active variable contract.
- Inspect the old Terraform state against the actual account before deciding
  whether to migrate, retain, or remove it. Never discard state that might map
  to live resources.
- Remove the tracked saved plan only in the approved cleanup pass; plans are
  environment-specific and may contain sensitive values.

Gate: secret scan passes and no active example asks for
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.

### 3. Make the source boundary reproducible

- Add a committed manifest definition for the official TLC URLs, exact local
  filenames, byte sizes, SHA-256 checksums, and selected profiles.
- Define `smoke` as one bounded 2024 month and `release` as the four consecutive
  locally staged 2024 months.
- Add a source preparation/validation script that checks filename, checksum,
  Parquet footer schema, selected profile, and S3 destination before upload.
- Persist manifest status and immutable source identity in a durable Iceberg
  operations table or another explicitly defined Glue-cataloged Iceberg table.

Gate: credential-independent tests reject changed checksums, missing months,
wrong schemas, and profile drift.

### 4. Complete Terraform and AWS authentication

- Retain the private, versioned, SSE-S3 project bucket.
- Align Glue Catalog database names with the identifiers used by Spark and dbt.
- Package all Python modules imported by each Glue entry point and pass the
  package through Glue job arguments.
- Add the temporary Airflow runner, instance role, and instance profile. Enforce
  IMDSv2 and use the profile credential chain; do not place AWS keys in user
  data or environment files.
- Add only the Athena resources defined in `docs/ATHENA_SCOPE.md`.
- Keep the bucket protected from accidental recursive deletion.

Gate: `terraform fmt -check`, `terraform validate`, policy tests, and a reviewed
`terraform plan` pass. A plan is not deployment evidence.

### 5. Make Bronze and Silver rerunnable

- Scope Bronze and Silver jobs to one year/month/checksum/run identity.
- Block a changed checksum unless a separately reviewed replacement workflow
  approves it.
- Replace blind append behavior with a tested Iceberg overwrite/merge strategy
  that preserves source-grain Bronze records and produces one canonical Silver
  `trip_id`.
- Pass all required month/run arguments from Airflow to both jobs.
- Add retry tests for failure before write, failure after Bronze publication,
  task clear, normal rerun, and forced same-checksum rerun.

Gate: deterministic tests reconcile `Bronze = Silver + quarantine` and show no
duplicate canonical rows after every retry path. Physical AWS idempotency stays
`NOT VERIFIED` until a real run exists.

### 6. Insert Great Expectations at the required boundary

- Add one small Great Expectations checkpoint over the month-scoped Bronze
  batch.
- Validate required columns, timestamp presence/order, non-negative measures,
  and resolvable zone IDs, while preserving invalid records for quarantine.
- Order the DAG as `Bronze -> Great Expectations -> Silver`.
- Retain the current post-Gold count/uniqueness checks as E2E reconciliation
  after dbt, under a non-Great-Expectations name.

Gate: the checkpoint passes the valid fixture, fails the controlled invalid
fixture, and the DAG cannot start Silver after checkpoint failure.

### 7. Complete dbt-glue publication

- Keep exactly three dimensions, one fact, and two marts.
- Align the dbt catalog/schema and S3 Gold location with Terraform outputs.
- Preserve uniqueness, not-null, relationship, and Silver-to-fact tests.
- Ensure reruns replace or merge the intended Gold state deterministically.

Gate: credential-independent `dbt parse` passes. Remote `dbt build` remains
`NOT VERIFIED` without AWS evidence.

### 8. Add the minimal Athena layer

- Implement exactly the Terraform, IAM, four SQL artifacts, generic Boto3
  runner, and Gold smoke verifier in `docs/ATHENA_SCOPE.md`.
- Do not add KMS, another bucket, Lake Formation, dashboards, alarms, an
  evidence exporter, a generic verification framework, or a large query pack.
- Keep time-travel verification manual and `NOT VERIFIED`.

Gate: SQL contract tests, mocked runner tests, Terraform checks, and a mocked
smoke-verifier test pass without credentials.

### 9. Finish deployment operations and CI

- Provide NYC-only scripts for source upload, provisioning preflight, Glue job
  package build, DAG configuration, E2E reconciliation, smoke/release runs,
  and teardown verification.
- Update CI to test Great Expectations contracts, Glue package contents,
  Airflow import/topology, dbt parse, Athena SQL/runner, IAM scope, and
  Terraform format/validation.
- Update the runbook to use Athena and clearly separate
  smoke, release, and teardown.

Gate: every credential-independent command passes from a fresh environment;
all cloud rows in deployment status remain `NOT VERIFIED`.

## Resume-ready closure gate

The codebase is resume-ready without a real deployment only when:

- the active repository expresses one architecture consistently;
- all required deployment artifacts are present and credential-independent
  checks pass;
- Terraform plans the bounded stack but has not been applied;
- source identity, row reconciliation, retry semantics, and teardown checks are
  explicit and tested locally;
- Athena is demonstrably bounded to the four requested SQL use cases;
- no static credential, account ID, private URL, state, or saved plan is
  tracked;
- README language says “deployment-ready code, AWS execution NOT VERIFIED,”
  never “deployed,” “production-ready,” or “exactly once.”

## Learning contract

Core decision: Great Expectations is a contract gate before Silver, while the
post-Gold reconciliation proves publication consistency. They answer different
questions and neither replaces Iceberg idempotency.

Teach-back questions:

1. Why must a Bronze contract failure block Silver while still preserving the
   source record in Bronze?
2. Why does an Athena workgroup bytes cutoff control query cost but not prove
   that Gold data is correct?
3. Why can a Terraform plan be resume evidence for infrastructure-as-code but
   not evidence of a working AWS lakehouse?

Failure/rerun experiment for the implementation pass: make the smoke manifest
checksum differ by one character, verify the run is blocked before Glue, then
restore the pinned checksum and verify the same preparation step succeeds
without changing the stable run identity.
