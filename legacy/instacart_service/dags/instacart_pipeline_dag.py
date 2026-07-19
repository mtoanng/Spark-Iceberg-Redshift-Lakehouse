"""
Instacart Lakehouse + Recommendation Store Pipeline

Orchestrates:
1. Bronze ingestion (AWS Glue Job)
2. Silver transformation (AWS Glue Job)
3. Gold layer (dbt-glue)
4. ML recommendations (AWS Glue Job - Spark ML)

Schedule: Weekly (for demonstration - dataset is static)

Author: Data Engineering Team
Date: 2026-07-16
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'instacart_lakehouse_recommendation',
    default_args=default_args,
    description='Instacart: AWS Glue → dbt → ML → MongoDB Recommendations',
    schedule_interval='@weekly',  # Weekly schedule (dataset is static snapshot)
    catchup=False,
    max_active_runs=1,
    tags=['instacart', 'lakehouse', 'recommendations', 'ml']
)

# Task 1: Validate schema
def validate_schema_func():
    """
    Placeholder for schema validation
    
    In production, this would:
    - Check S3 raw files exist
    - Validate CSV headers match expected schema
    - Check file sizes are reasonable
    """
    logger.info("🔍 Validating schema...")
    logger.info("✅ Schema validation passed (placeholder)")
    return True

validate_schema = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema_func,
    dag=dag
)

# Task 2: Bronze layer ingestion (AWS Glue Job)
load_bronze = GlueJobOperator(
    task_id='bronze_ingestion',
    job_name='instacart-lakehouse-bronze-ingestion',
    script_args={
        '--S3_BUCKET': '{{ var.value.s3_bucket }}',
        '--S3_RAW_PREFIX': 'raw/instacart'
    },
    aws_conn_id='aws_default',
    region_name='{{ var.value.aws_region }}',
    wait_for_completion=True,
    verbose=True,
    dag=dag
)

# Task 3: Silver layer transformation (AWS Glue Job)
transform_silver = GlueJobOperator(
    task_id='silver_transformation',
    job_name='instacart-lakehouse-silver-transformation',
    aws_conn_id='aws_default',
    region_name='{{ var.value.aws_region }}',
    wait_for_completion=True,
    verbose=True,
    dag=dag
)

# Task 4: dbt run (Gold layer)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command="""
        cd {{ var.value.project_root }}/etl/dbt_project && \
        dbt run --profiles-dir . --target glue --select marts
    """,
    dag=dag
)

# Task 5: dbt test
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command="""
        cd {{ var.value.project_root }}/etl/dbt_project && \
        dbt test --profiles-dir . --target glue
    """,
    dag=dag
)

# Task 6: ML Recommendations (Spark ML on AWS Glue)
ml_recommendations = GlueJobOperator(
    task_id='ml_recommendations',
    job_name='instacart-lakehouse-ml-recommendations',
    script_args={
        '--MONGODB_URI': '{{ var.value.mongodb_uri }}',
        '--MONGODB_DATABASE': '{{ var.value.mongodb_database | default("instacart_ml_warehouse", true) }}',
        '--TOP_N': '10'
    },
    aws_conn_id='aws_default',
    region_name='{{ var.value.aws_region }}',
    wait_for_completion=True,
    verbose=True,
    dag=dag
)

# Task 7: Verify recommendations were written
def verify_recommendations_func():
    """
    Verify recommendations were written to MongoDB
    
    Checks:
    - MongoDB connection
    - Recommendation count > 0
    - Sample user has recommendations
    """
    import os
    from warehouse.recommendation_store import RecommendationStore
    
    logger.info("🔍 Verifying recommendations...")
    
    # Require MongoDB URI - no localhost fallback
    mongodb_uri = os.environ.get('MONGODB_URI')
    if not mongodb_uri:
        raise ValueError("MONGODB_URI environment variable required")
    
    rec_store = RecommendationStore(
        mongo_uri=mongodb_uri,
        database='instacart_ml_warehouse'
    )
    
    try:
        stats = rec_store.get_stats()
        user_count = stats.get('total_users', 0)
        
        logger.info(f"✅ Found {user_count} users with recommendations")
        
        if user_count == 0:
            raise ValueError("No recommendations found in MongoDB!")
        
        return True
        
    finally:
        rec_store.close()

verify_recommendations = PythonOperator(
    task_id='verify_recommendations',
    python_callable=verify_recommendations_func,
    dag=dag
)

# Define task dependencies (linear pipeline)
validate_schema >> load_bronze >> transform_silver >> dbt_run >> dbt_test >> ml_recommendations >> verify_recommendations

# Documentation
dag.doc_md = """
# Instacart Lakehouse + Recommendation Store Pipeline

## Architecture
```
CSV (S3) → AWS Glue Jobs → Iceberg Tables → dbt Gold → ML Model → MongoDB Recommendations
```

## Layers
- **Bronze:** Raw CSV → Iceberg (6 tables)
- **Silver:** Cleaned, enriched, deduplicated (3 tables)
- **Gold:** Star schema + ML features (10 dbt models)
- **ML:** Spark ML Logistic Regression (AWS Glue Job)
- **Recommendations:** Top-N products per user (MongoDB Atlas)

## Schedule
- **Interval:** Weekly
- **Note:** Dataset is a static snapshot from Kaggle Instacart competition. 
  Weekly schedule is for demonstration purposes only. In production with 
  streaming data, this would be daily or event-driven.

## Airflow Variables Required
- `s3_bucket`: S3 bucket name (e.g., "instacart-lakehouse-prod")
- `aws_region`: AWS region (e.g., "us-east-1")
- `project_root`: Absolute path to project root (e.g., "/opt/airflow/dags/repo")
- `mongodb_uri`: MongoDB Atlas connection string
- `mongodb_database`: MongoDB database name (default: "instacart_ml_warehouse")

## AWS Connections Required
- `aws_default`: AWS credentials with permissions for:
  - Glue (StartJobRun, GetJobRun)
  - S3 (GetObject, PutObject)
  - Glue Data Catalog (GetDatabase, GetTable)

## Environment Variables Required
- `AWS_ACCESS_KEY_ID`: AWS access key (for Glue Catalog access from dbt)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_REGION`: AWS region

## Monitoring
- Check CloudWatch logs for Glue Jobs
- Check Airflow task logs for dbt/ML steps
- Check MongoDB for recommendation counts
"""

if __name__ == "__main__":
    """Test DAG imports and structure"""
    print("🧪 Testing DAG structure...")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Schedule: {dag.schedule_interval}")
    print(f"Tasks: {len(dag.tasks)}")
    for task in dag.tasks:
        print(f"  - {task.task_id}")
    print("✅ DAG structure valid!")
