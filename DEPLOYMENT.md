# Deployment Guide

**Production-ready deployment for Databricks Spark jobs**

---

## 🎯 Overview

This guide covers deploying PySpark jobs to Databricks for scheduled execution (not manual notebook uploads).

---

## 📦 Job Structure

### PySpark Jobs (Professional Pattern)

```
pyspark/
├── bronze_ingestion.py        # Standalone script
├── silver_transformation.py   # Standalone script
├── data_quality_checks.py     # Standalone script
└── utils.py                   # Shared utilities
```

**Key Design:**
- ✅ Standalone Python scripts (not notebooks)
- ✅ Can run via `spark-submit`
- ✅ Configuration via environment variables
- ✅ Proper error handling and logging
- ✅ Can be deployed as Databricks Jobs

---

## 🚀 Deployment Options

### Option 1: Databricks Jobs API (Recommended)

```bash
# 1. Package your code
zip -r instacart_pipeline.zip pyspark/ config/

# 2. Upload to DBFS
databricks fs cp instacart_pipeline.zip dbfs:/jobs/instacart_pipeline.zip

# 3. Create job via API
curl -X POST ${DATABRICKS_HOST}/api/2.0/jobs/create \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -d '{
    "name": "Instacart Bronze Ingestion",
    "existing_cluster_id": "'"${DATABRICKS_CLUSTER_ID}"'",
    "spark_python_task": {
      "python_file": "dbfs:/jobs/instacart_pipeline/pyspark/bronze_ingestion.py"
    },
    "libraries": [
      {"pypi": {"package": "pyiceberg>=0.5.0"}}
    ]
  }'
```

### Option 2: Databricks CLI

```bash
# Install CLI
pip install databricks-cli

# Configure
databricks configure --token

# Create job
databricks jobs create --json-file job_config.json

# Run job
databricks jobs run-now --job-id <job_id>
```

### Option 3: Terraform (IaC)

```hcl
resource "databricks_job" "bronze_ingestion" {
  name = "Instacart Bronze Ingestion"
  
  existing_cluster_id = var.databricks_cluster_id
  
  spark_python_task {
    python_file = "dbfs:/jobs/instacart_pipeline/pyspark/bronze_ingestion.py"
  }
  
  library {
    pypi {
      package = "pyiceberg>=0.5.0"
    }
  }
  
  email_notifications {
    on_failure = ["data-team@company.com"]
  }
}
```

---

## 📋 Job Configuration Template

Create `databricks_jobs/bronze_ingestion.json`:

```json
{
  "name": "Instacart Bronze Ingestion",
  "existing_cluster_id": "${DATABRICKS_CLUSTER_ID}",
  "spark_python_task": {
    "python_file": "dbfs:/jobs/instacart_pipeline/pyspark/bronze_ingestion.py",
    "parameters": []
  },
  "libraries": [
    {"pypi": {"package": "pyiceberg>=0.5.0"}},
    {"pypi": {"package": "boto3>=1.28.0"}}
  ],
  "email_notifications": {
    "on_start": [],
    "on_success": [],
    "on_failure": ["alerts@example.com"]
  },
  "timeout_seconds": 3600,
  "max_retries": 2,
  "min_retry_interval_millis": 60000
}
```

---

## 🔧 Environment Configuration

### Databricks Secrets (Recommended)

```bash
# Create secret scope
databricks secrets create-scope --scope instacart-prod

# Add secrets
databricks secrets put --scope instacart-prod --key aws_access_key_id
databricks secrets put --scope instacart-prod --key aws_secret_access_key
databricks secrets put --scope instacart-prod --key mongodb_uri
```

### Update PySpark Scripts to Use Secrets

```python
# In bronze_ingestion.py
from pyspark.dbutils import DBUtils

dbutils = DBUtils(spark)

# Get secrets
aws_key = dbutils.secrets.get(scope="instacart-prod", key="aws_access_key_id")
aws_secret = dbutils.secrets.get(scope="instacart-prod", key="aws_secret_access_key")

# Use in Spark config
spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret)
```

---

## 📦 Packaging for Deployment

### Create Wheel Package (Professional)

```bash
# Project structure
instacart_pipeline/
├── setup.py
├── instacart_pipeline/
│   ├── __init__.py
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   └── config.py

# setup.py
from setuptools import setup, find_packages

setup(
    name="instacart-pipeline",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pyiceberg>=0.5.0",
        "boto3>=1.28.0"
    ]
)

# Build wheel
python setup.py bdist_wheel

# Upload to DBFS
databricks fs cp dist/instacart_pipeline-1.0.0-py3-none-any.whl \
  dbfs:/libraries/instacart_pipeline-1.0.0-py3-none-any.whl

# Use in job
{
  "libraries": [
    {"whl": "dbfs:/libraries/instacart_pipeline-1.0.0-py3-none-any.whl"}
  ]
}
```

---

## 🔄 CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy to Databricks

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install databricks-cli
      
      - name: Configure Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          echo "$DATABRICKS_HOST" > ~/.databrickscfg
          echo "$DATABRICKS_TOKEN" >> ~/.databrickscfg
      
      - name: Upload code to DBFS
        run: |
          zip -r pipeline.zip pyspark/ config/
          databricks fs cp pipeline.zip dbfs:/jobs/instacart_pipeline.zip --overwrite
      
      - name: Run job
        run: |
          databricks jobs run-now --job-id ${{ secrets.JOB_ID }}
```

---

## 🧪 Testing Before Deployment

```bash
# Local testing
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export S3_BUCKET=instacart-lakehouse

# Run locally with spark-submit
spark-submit \
  --master local[*] \
  --driver-memory 4g \
  pyspark/bronze_ingestion.py

# Test on Databricks (one-time run)
databricks runs submit --json '{
  "run_name": "Test Bronze Ingestion",
  "existing_cluster_id": "'"${DATABRICKS_CLUSTER_ID}"'",
  "spark_python_task": {
    "python_file": "dbfs:/jobs/test/bronze_ingestion.py"
  }
}'
```

---

## 📊 Monitoring & Logging

### Built-in Spark Logging

```python
import logging

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Use in code
logger.info("Starting Bronze ingestion")
logger.warning(f"Row count mismatch: {actual} vs {expected}")
logger.error(f"Failed to process table: {str(e)}")
```

### Databricks Job Metrics

```bash
# Get run details
databricks runs get --run-id <run_id>

# Get run output
databricks runs get-output --run-id <run_id>
```

---

## 🔐 Security Best Practices

1. **Never hardcode credentials**
   - Use Databricks Secrets
   - Use IAM roles (if on AWS Databricks)

2. **Use separate clusters for dev/prod**
   ```bash
   dev_cluster_id=xxx-xxx-xxx
   prod_cluster_id=yyy-yyy-yyy
   ```

3. **Limit access via ACLs**
   ```bash
   databricks jobs update --job-id <id> --permissions '[
     {"user_name": "data-team@company.com", "permission_level": "CAN_MANAGE"}
   ]'
   ```

---

## 📝 Deployment Checklist

- [ ] Code packaged as standalone scripts
- [ ] Dependencies listed in requirements.txt or setup.py
- [ ] Secrets stored in Databricks Secrets
- [ ] Job JSON config created
- [ ] Code uploaded to DBFS
- [ ] Job created via API/CLI/Terraform
- [ ] Test run successful
- [ ] Email notifications configured
- [ ] Retry policy set
- [ ] Monitoring configured

---

## 🎯 Production Workflow

```
1. Developer commits code
2. CI/CD pipeline triggers
3. Tests run
4. Code packaged (wheel or zip)
5. Upload to DBFS
6. Update Databricks Job
7. Trigger test run
8. Monitor execution
9. Alert on failures
```

---

## 📚 Resources

- [Databricks Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [Secrets Management](https://docs.databricks.com/security/secrets/index.html)
- [Python Wheel Packaging](https://packaging.python.org/guides/distributing-packages-using-setuptools/)

---

**This is a production-ready deployment pattern, not manual notebook execution!**
