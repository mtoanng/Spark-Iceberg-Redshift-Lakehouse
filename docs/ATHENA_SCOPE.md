# Athena boundary

Athena is an independent, read-only verifier for the open Iceberg layers. It
does not transform data and does not query managed Redshift Gold.

Allowed runtime queries:

- month-scoped Bronze count;
- month-scoped Silver count;
- month-scoped quarantine count;
- one operational-manifest row for the requested stable run;
- post-publication Silver count;
- the explicit 2024 snapshot version-travel query after 2025 evolution.

The Terraform workgroup enforces its result location and per-query bytes
cutoff. IAM limits catalog access to `bronze`, `silver`, and `ops`, and S3
access to their warehouse prefixes plus Athena results.

Redshift Data API owns Gold reconciliation and verification. This separation
makes the cross-engine gate explicit.
