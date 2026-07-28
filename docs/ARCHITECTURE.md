# Architecture

```text
S3 landing (input contract)
        |
        v
regular MWAA / Airflow 3
        |
        +--> EMR Serverless Bronze --------+
        |                                  |
        +--> EMR Serverless Silver/Q ------+--> S3 Iceberg
                                               |
                                          Glue Catalog
                                           /         \
                                      Athena       Spectrum
                                                      |
                                              Redshift Serverless
                                                      |
                                                 dbt managed Gold
                                                      |
                                      reconcile -> publish -> verify
```

## Why the components are cohesive

Storage, compute, metadata, orchestration, and serving are intentionally
separate:

- S3/Iceberg is the canonical open data plane.
- Glue is the shared metadata boundary.
- EMR Serverless performs Spark work and owns no persistent cluster.
- Redshift Spectrum reads the same Silver tables without copying them first.
- dbt creates the managed serving model inside Redshift.
- MWAA controls stage ordering but contains no transformation logic.
- Athena is a bounded, read-only independent path for open-layer evidence.

## Network and authentication

MWAA and Redshift Serverless use the same existing VPC and two private subnets.
Redshift accepts port 5439 only from the MWAA security group. MWAA uses its
execution role for AWS APIs and dbt uses Redshift Serverless IAM-role
authentication. Terraform creates the corresponding password-disabled Redshift
IAM user and grants only external-schema reads plus Gold schema create/usage.

Private subnets must provide required AWS API and Python package access through
NAT and/or VPC endpoints. This network is an explicit deployment prerequisite,
not provisioned by this repository.

## State boundaries

The only mutable operational state is `ops.source_run_manifest`. It records
the immutable source identity, stable run ID, current Bronze/Silver state,
counts, snapshots, and failure details.

Publication is separate durable release evidence. It is written only after dbt
and both reconciliation invariants pass. Verification reads the published
contract as a consumer would.

## Cost boundary

EMR Serverless auto-stops. Redshift Serverless bills for active work. Regular
MWAA has a provisioned baseline cost, so Terraform defaults to the smallest
chosen class and supplies a review-only bounded teardown plan. No teardown is
applied automatically.
