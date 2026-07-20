# Proposed deletions and removals

Audit updated: 2026-07-21

## Completed in Closure Phase B

| Removed target | Replacement and evidence |
| --- | --- |
| Retired local Gold consumer package | `athena/query_runner.py`, `athena/verify_gold.py`, and four bounded SQL artifacts. |
| Retired consumer unit/contract tests and in-memory Gold fixture | Mocked Boto3 runner/verifier tests plus SQL static tests. |
| Retired local query dependency | Boto3 is the active Athena SDK boundary; no active dependency or CI command refers to the retired engine. |

## Still deferred — do not remove in this phase

- `legacy/instacart_service/`, `legacy/dbt_instacart_models/`,
  `legacy/terraform_instacart/`, and `legacy/docs_instacart/` remain migration
  history until separately approved.
- Old scripts, ignored `.env`, private tfvars, Terraform state, plan/cache, and
  generated dbt/Spark artifacts require a separate operator-reviewed cleanup.
- Never delete a state file until live-resource ownership has been confirmed.

Removing a local file does not revoke credentials or remove prior history. If
the ignored environment or policy artifacts contained real credentials, rotate
them before any AWS deployment.
