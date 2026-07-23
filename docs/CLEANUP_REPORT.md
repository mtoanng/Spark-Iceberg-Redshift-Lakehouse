# Cleanup report

## Removed

- Tracked partial `.venv` files and generated Terraform plan material.
- Active 2025 schema-evolution Glue job and advanced Iceberg lifecycle module.
- Obsolete local publication writer replaced by the manifest Glue job.
- Stale active documentation describing legacy datasets, recommendation/ML
  work, removed lifecycle code, and old dbt targets.

## Archived

- Nothing newly archived. Historical phase reports remain clearly historical.

## Kept

- Official NYC TLC source-shaped fixtures, including a small 2025 schema
  fixture used only for contract readability.
- Exactly six Gold models, four bounded Athena query artifacts, and protected
  canonical S3/Glue resources.
- Historical reports needed to explain prior verification work.

## Reason

The active repository now describes only the bounded NYC HVFHV lifecycle. The
first AWS deployment does not need automated schema evolution, maintenance,
alternate serving engines, or legacy dataset code.
