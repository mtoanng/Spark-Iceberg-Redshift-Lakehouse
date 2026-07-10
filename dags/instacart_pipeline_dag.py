"""
Airflow DAG - Instacart Lakehouse Pipeline
Orchestrates the end-to-end data pipeline from raw CSV to Gold layer

DAG Steps:
1. Download CSV from Kaggle (manual/automated)
2. Upload to S3 raw layer
3. Bronze ingestion (Spark: CSV → Iceberg Bronze on S3)
4. Silver transformation (Spark: Bronze → Iceberg Silver with cleaning)
5. Data quality checks
6. dbt run (Silver → Gold dimensional model on Databricks)
7. Register metadata to MongoDB catalog

Schedule: Weekly (every Monday at 2 AM)
Author: Data Engineering Team
Date: 2026-07-10
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 10),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='instacart_lakehouse_pipeline',
    default_args=default_args,
    description='Instacart data pipeline: CSV → S3 → Iceberg (Bronze/Silver) → dbt (Gold) → MongoDB Metadata',
    schedule_interval='0 2 * * 1',  # Every Monday 2 AM
    catchup=False,
    tags=['instacart', 'lakehouse', 'batch', 's3', 'iceberg'],
)


# Task 1: Check if raw data exists in S3
check_raw_data = BashOperator(
    task_id='check_raw_data_exists',
    bash_command='aws s3 ls s3://{{ var.value.s3_bucket }}/raw/instacart/ --recursive | wc -l',
    dag=dag,
)

# Task 2: Upload to S3 (if needed)
upload_to_s3 = BashOperator(
    task_id='upload_raw_data_to_s3',
    bash_command='python {{ var.value.project_root }}/scripts/upload_to_s3.py',
    dag=dag,
)

# Task Group: Bronze Layer Ingestion
with TaskGroup('bronze_ingestion', tooltip='Bronze layer ingestion', dag=dag) as bronze_group:
    
    run_bronze_ingestion = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command=(
            'spark-submit '
            '--master local[*] '
            '--driver-memory 4g '
            '--executor-memory 4g '
            '{{ var.value.project_root }}/pyspark/bronze_ingestion.py'
        ),
    )
    
    validate_bronze_tables = BashOperator(
        task_id='validate_bronze_tables',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/validate_iceberg_tables.py '
            '--layer bronze'
        ),
    )
    
    run_bronze_ingestion >> validate_bronze_tables

# Task Group: Silver Layer Transformation
with TaskGroup('silver_transformation', tooltip='Silver layer transformation', dag=dag) as silver_group:
    
    run_silver_transformation = BashOperator(
        task_id='run_silver_transformation',
        bash_command=(
            'spark-submit '
            '--master local[*] '
            '--driver-memory 4g '
            '--executor-memory 4g '
            '{{ var.value.project_root }}/pyspark/silver_transformation.py'
        ),
    )
    
    validate_silver_tables = BashOperator(
        task_id='validate_silver_tables',
        bash_command=(
            'python {{ var.value.project_root }}/scripts/validate_iceberg_tables.py '
            '--layer silver'
        ),
    )
    
    run_silver_transformation >> validate_silver_tables

# Task Group: Data Quality Checks
with TaskGroup('data_quality_checks', tooltip='Data quality validation', dag=dag) as dq_group:
    
    run_pyspark_dq_checks = BashOperator(
        task_id='run_pyspark_quality_checks',
        bash_command=(
            'spark-submit '
            '--master local[*] '
            '{{ var.value.project_root }}/pyspark/data_quality_checks.py'
        ),
    )

# Task Group: dbt Run & Test (on Databricks)
with TaskGroup('dbt_build', tooltip='dbt transformations on Databricks', dag=dag) as dbt_group:
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd {{ var.value.project_root }}/dbt_instacart && dbt deps --profiles-dir ~/.dbt',
    )
    
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd {{ var.value.project_root }}/dbt_instacart && dbt run --select staging --target prod',
    )
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd {{ var.value.project_root }}/dbt_instacart && dbt run --select marts --target prod',
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd {{ var.value.project_root }}/dbt_instacart && dbt test --target prod',
    )
    
    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test


# Task: Register metadata to MongoDB
register_metadata = BashOperator(
    task_id='register_metadata_to_mongodb',
    bash_command='python {{ var.value.project_root }}/scripts/register_metadata.py',
    dag=dag,
)


# Task: Generate data documentation
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='cd {{ var.value.project_root }}/dbt_instacart && dbt docs generate --target prod',
    dag=dag,
)

# Task: Send success notification
success_notification = BashOperator(
    task_id='send_success_notification',
    bash_command='echo "✅ Instacart pipeline completed successfully at $(date)"',
    dag=dag,
)

# Define task dependencies
check_raw_data >> upload_to_s3 >> bronze_group
bronze_group >> silver_group
silver_group >> dq_group
dq_group >> dbt_group
dbt_group >> register_metadata >> generate_docs >> success_notification


# Configuration notes:
"""
Required Airflow Variables (set in Airflow UI):
- s3_bucket: S3 bucket name (e.g., 'instacart-lakehouse')
- project_root: Absolute path to project root

Example:
airflow variables set s3_bucket "instacart-lakehouse"
airflow variables set project_root "/home/user/Data-Migration-with-Spark-Airflow-Postgres"
"""
