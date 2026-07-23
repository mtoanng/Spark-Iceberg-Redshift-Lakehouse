#!/usr/bin/env bash
set -euo pipefail

# The EC2 instance profile is the only AWS credential source. IMDSv2 is
# enforced by Terraform; no access-key environment variables are accepted.
test -z "${AWS_ACCESS_KEY_ID:-}" || { echo "static AWS keys are forbidden" >&2; exit 1; }
test -z "${AWS_SECRET_ACCESS_KEY:-}" || { echo "static AWS keys are forbidden" >&2; exit 1; }
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
install -d -m 0750 "$AIRFLOW_HOME/dags" "$AIRFLOW_HOME/project"
echo "Bootstrap complete; install the reviewed image and DAG package before starting Airflow."
