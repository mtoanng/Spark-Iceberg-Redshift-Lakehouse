#!/bin/bash
set -euo pipefail

DBT_VENV=/usr/local/airflow/dbt_venv
python3 -m venv "$DBT_VENV"
"$DBT_VENV/bin/python" -m pip install --no-cache-dir \
  dbt-core==1.10.19 \
  dbt-redshift==1.10.2
"$DBT_VENV/bin/dbt" --version
