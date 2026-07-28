# Bounded AWS evidence

Status: **NOT VERIFIED** until populated from one retained AWS run.

## Source and identity

- Year/month:
- S3 URI:
- SHA-256 metadata:
- Byte size:
- Stable run ID:
- Identity policy version:

## Processing

- MWAA DAG run:
- Bronze EMR job ID / count / snapshot:
- Silver EMR job ID / count / snapshot:
- Quarantine count / snapshot / reasons:
- `Bronze = Silver + quarantine`: PASS / FAIL

## Gold and release

- dbt invocation / artifact URI / SHA-256:
- Six Gold relations present: PASS / FAIL
- Gold fact count:
- `Silver = Gold`: PASS / FAIL
- Athena query IDs:
- Redshift statement ID:
- Publication URI / SHA-256:
- Read-after-publish verification: PASS / FAIL

## Operational proof

- Cleared-task retry:
- Identical monthly rerun:
- Changed-identity rejection:
- Four-month sequence:
- 2025 evolution and 2024 version travel:
- Bounded teardown:

Never place credentials, account IDs, private endpoints, raw source data,
Terraform state, or saved plans in retained evidence.
