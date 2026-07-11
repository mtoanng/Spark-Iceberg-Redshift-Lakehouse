# Setup Guide - Instacart Lakehouse

**AWS S3 + Spark OSS + MongoDB + DuckDB**

---

## Quick Start (3 Commands)

```bash
python scripts/setup_kaggle.py && python scripts/download_kaggle_dataset.py
cd terraform && terraform apply
python scripts/upload_to_s3.py
# Then run pipeline via spark-submit or Airflow
```

---

## Detailed Setup

### Phase 1: Local (30 min)
1. Install Python dependencies: `pip install -r requirements.txt`
2. Setup Kaggle: `python scripts/setup_kaggle.py`
3. Download data: `python scripts/download_kaggle_dataset.py`

### Phase 2: Cloud Accounts (20 min)
- AWS account (S3 free tier)
- Configure AWS credentials: `export AWS_ACCESS_KEY_ID=xxx`

### Phase 3: Infrastructure (10 min)
```bash
cd terraform
terraform init && terraform apply
```

### Phase 4: Upload Data (10 min)
```bash
python scripts/upload_to_s3.py
```

### Phase 5: Spark Pipeline (30 min)
```bash
# Bronze ingestion
spark-submit --master local[*] pyspark/bronze_ingestion.py

# Silver transformation
spark-submit --master local[*] pyspark/silver_transformation.py

# Data quality checks
spark-submit --master local[*] pyspark/data_quality_checks.py
```

### Phase 6: dbt (15 min)
```bash
cd dbt_instacart
dbt run --profiles-dir . --target prod && dbt test --profiles-dir .
```

### Phase 7: Warehouse API (5 min)
```bash
docker-compose up -d    # Starts MongoDB + Warehouse API
curl http://localhost:8000/health
```

---

## Verification

```bash
# Check S3
aws s3 ls s3://your-bucket/bronze/

# Check Warehouse API
curl http://localhost:8000/health
curl http://localhost:8000/datasets
```

---

## Troubleshooting

**Spark can't read S3:**
```python
# Ensure AWS credentials are set in environment
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
# Or configure in config/instacart_config.py
```

**MongoDB connection refused:**
```bash
# Ensure Docker is running and MongoDB container is up
docker-compose up -d mongodb
docker-compose ps
```

---

See [README.md](README.md) for architecture overview.
