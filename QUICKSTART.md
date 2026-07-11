# Quick Start Guide

**Fast track to get pipeline running (assumes you have accounts)**

---

## 🚀 30-Minute Quick Start

### Prerequisites
- AWS account with credentials
- Databricks on AWS (trial via AWS Marketplace)
- MongoDB Atlas account (free tier)
- Python 3.9+ installed

---

## Step 1: Install Dependencies (5 min)

```bash
# Create environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install
pip install -r requirements.txt
```

---

## Step 2: Setup Credentials (5 min)

```bash
# Copy template
cp .env.example .env

# Edit .env with your credentials:
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY
# - DATABRICKS_TOKEN
# - DATABRICKS_CLUSTER_ID
# - MONGODB_URI
```

---

## Step 3: Deploy Infrastructure (3 min)

```bash
cd terraform
terraform init
terraform apply -auto-approve

# Save output values!
terraform output s3_bucket_name
cd ..
```

---

## Step 4: Upload Data to S3 (10 min)

```bash
# Download Instacart dataset
python scripts/download_kaggle_dataset.py

# Upload to S3
python scripts/upload_to_s3.py
```

---

## Step 5: Run Pipeline (5 min setup)

```bash
# Package code
zip -r pipeline.zip pyspark/ config/

# Upload to Databricks
databricks fs cp pipeline.zip dbfs:/jobs/instacart_pipeline.zip --overwrite

# Create and run Bronze job
databricks jobs create --json-file databricks_jobs/bronze_job.json
databricks jobs run-now --job-id [job-id]

# Monitor in Databricks UI
# Then repeat for Silver, Gold (dbt)
```

---

## Step 6: Start Warehouse API (2 min)

```bash
# Register metadata
python scripts/register_metadata.py

# Start API
cd warehouse
uvicorn main:app --reload --port 8000

# Test: http://localhost:8000/docs
```

---

## Quick Test

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient()
df = client.query("SELECT * FROM gold.dim_product LIMIT 10")
print(df)
```

---

## 🆘 Quick Troubleshooting

**Issue:** AWS credentials error  
**Fix:** `aws configure` and re-enter credentials

**Issue:** Databricks cluster not found  
**Fix:** Start cluster in UI, wait 2-3 minutes

**Issue:** MongoDB connection fails  
**Fix:** Check connection string format and IP whitelist

**Issue:** S3 access denied  
**Fix:** Verify IAM policy has S3 permissions

---

## 📖 Full Guide

See `TODO.md` for complete step-by-step guide with all details.

---

## 💡 Key Commands Reference

```bash
# Terraform
terraform init
terraform plan
terraform apply
terraform destroy

# AWS
aws s3 ls s3://your-bucket/
aws s3 cp file.csv s3://your-bucket/path/

# Databricks
databricks clusters list
databricks jobs list
databricks jobs run-now --job-id [id]

# dbt
dbt debug --profiles-dir ~/.dbt
dbt run --profiles-dir ~/.dbt
dbt test --profiles-dir ~/.dbt

# Warehouse
uvicorn main:app --reload --port 8000
```

---

**Time to first query: ~30 minutes** 🎯
