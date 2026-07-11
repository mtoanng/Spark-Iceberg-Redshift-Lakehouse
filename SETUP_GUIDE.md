# Setup Guide - Instacart Lakehouse

**Hybrid Cloud: AWS S3 + Databricks + GCP BigQuery**

---

## Quick Start (3 Commands)

```bash
python scripts/setup_kaggle.py && python scripts/download_kaggle_dataset.py
cd terraform && terraform apply
python scripts/upload_to_s3.py
# Then run on Databricks
```

---

## Detailed Setup

### Phase 1: Local (30 min)
1. Install Python dependencies: `pip install -r requirements.txt`
2. Setup Kaggle: `python scripts/setup_kaggle.py`
3. Download data: `python scripts/download_kaggle_dataset.py`

### Phase 2: Cloud Accounts (20 min)
- AWS account (S3 free tier)
- Databricks on AWS (trial via AWS Marketplace, 14-day)

### Phase 3: Infrastructure (10 min)
```bash
cd terraform
terraform init && terraform apply
```

### Phase 4: Upload Data (10 min)
```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
python scripts/upload_to_s3.py
```

### Phase 5: Databricks (30 min)
- Create cluster (Runtime 13.3 LTS)
- Install libraries: Iceberg + hadoop-aws
- Upload scripts to DBFS
- Run: Bronze → Silver → Export

### Phase 6: dbt (15 min)
```bash
cd dbt_instacart
dbt run && dbt test
```

---

## Verification

```bash
# Check S3
aws s3 ls s3://your-bucket/bronze/

# Check BigQuery  
bq query "SELECT COUNT(*) FROM instacart_lakehouse.fct_order_products"
```

---

## Troubleshooting

**Databricks can't read S3:**
```python
# Set credentials in notebook
spark.conf.set("spark.hadoop.fs.s3a.access.key", "xxx")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "xxx")
```

**BigQuery access denied:**
```bash
# Verify service account permissions
gcloud projects get-iam-policy your-project-id
```

---

See [README.md](README.md) for architecture overview.
