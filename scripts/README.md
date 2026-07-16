# Scripts Directory

Utility scripts for data management and operational tasks.

---

## Quick Start (Local — No Cloud Required)

### `setup_kaggle.py`
Interactive Kaggle API setup and validation.

```bash
python scripts/setup_kaggle.py
```

Creates `~/.kaggle/`, guides credential setup, tests API authentication, validates competition access.

### `download_kaggle_dataset.py`
Download Instacart dataset from Kaggle (~1.3GB, 6 CSV files).

```bash
pip install kaggle
python scripts/download_kaggle_dataset.py
```

**Output:** Raw CSV files in `data/raw/instacart/`

### `explore_data_local.py`
Comprehensive local data exploration and quality validation.

```bash
python scripts/explore_data_local.py
```

Analyzes file validation, reference tables, products hierarchy, order patterns, top products, and data quality summary.

---

## Cloud Deployment (Requires AWS S3 Setup)

### `upload_to_s3.py`
Upload CSV files from local to S3 raw layer.

**Prerequisites:**
- S3 bucket created (via Terraform)
- AWS credentials configured (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

```bash
python scripts/upload_to_s3.py
```

### `validate_iceberg_tables.py`
Validate Iceberg tables exist and have data.

```bash
python scripts/validate_iceberg_tables.py --layer bronze
python scripts/validate_iceberg_tables.py --layer silver
### Production Scripts

Essential scripts for deployment:

#### `download_kaggle_dataset.py`
Download Instacart dataset from Kaggle.

```bash
python scripts/download_kaggle_dataset.py
```

#### `upload_to_s3.py`
Upload raw CSV files to S3 bucket.

```bash
python scripts/upload_to_s3.py
```

---

### Development/Testing Scripts

Optional scripts for local development:

#### `explore_data_local.py`
Explore dataset structure locally before uploading to S3.

#### `validate_iceberg_tables.py`
Validate Iceberg table metadata and schemas.

---

## Quick Workflow

```bash
# 1. Download dataset
python scripts/download_kaggle_dataset.py

# 2. Upload to S3
python scripts/upload_to_s3.py

# 3. Continue with Terraform & Glue Jobs
# See: SETUP_CHECKLIST_A_TO_Z.md
```

Reads table stats (row count, schema, location) from Spark and writes metadata documents to MongoDB.

---

## Typical Workflow

### Phase 1: Local Setup (No Cloud)
```bash
python scripts/setup_kaggle.py        # 1. Setup Kaggle API
python scripts/download_kaggle_dataset.py  # 2. Download dataset
python scripts/explore_data_local.py   # 3. Explore locally
```

### Phase 2: Cloud Pipeline
```bash
python scripts/upload_to_s3.py         # 4. Upload to S3

# 5. Run Bronze ingestion
spark-submit --master local[*] pyspark/bronze_ingestion.py
python scripts/validate_iceberg_tables.py --layer bronze

# 6. Run Silver transformation
spark-submit --master local[*] pyspark/silver_transformation.py
python scripts/validate_iceberg_tables.py --layer silver

# 7. Run data quality checks
spark-submit --master local[*] pyspark/data_quality_checks.py

# 8. Run dbt (build dimensional model)
cd etl/dbt_project && dbt run --profiles-dir . --target glue
cd etl/dbt_project && dbt test --profiles-dir . --target glue
# 9. Done! API ready
# See: SETUP_CHECKLIST_A_TO_Z.md for deployment
```

---

## Tips

- **Start local first:** Run Phase 1 to understand data before cloud deployment
- **Spark execution:** PySpark scripts run via `spark-submit --master local[*]` (local dev) or on EC2 (deploy)
- **Validation scripts:** Can run locally with proper AWS credentials
- **Incremental development:** Test each layer before proceeding to next

---

## Related Documentation

- [QUICKSTART.md](../QUICKSTART.md) — 30-minute quickstart guide
- [CODEBASE_WALKTHROUGH.md](../CODEBASE_WALKTHROUGH.md) — Full architecture walkthrough
- [SETUP_GUIDE.md](../SETUP_GUIDE.md) — AWS + MongoDB setup
# Scripts Directory

Utility scripts for data management and operational tasks.

---

## 🚀 Quick Start (Local - No Cloud Required)

### 1. `setup_kaggle.py` ⭐ NEW
Interactive Kaggle API setup and validation.

**Usage:**
```bash
python scripts/setup_kaggle.py
```

**What it does:**
- Creates `~/.kaggle/` directory
- Guides you through credential setup
- Tests API authentication
- Validates competition access

**Output:** Configured Kaggle API ready to download datasets

---

### 2. `download_kaggle_dataset.py`
Download Instacart dataset from Kaggle competition.

**Prerequisites:**
```bash
pip install kaggle
```

**Setup Kaggle API:**
1. Get API credentials from https://www.kaggle.com/settings/account
2. Save to `~/.kaggle/kaggle.json`
3. Accept competition rules: https://www.kaggle.com/c/instacart-market-basket-analysis/rules

**Usage:**
```bash
python scripts/download_kaggle_dataset.py
```

### 2. `download_kaggle_dataset.py`
Download Instacart dataset from Kaggle competition.

**Prerequisites:**
```bash
pip install kaggle  # Already installed ✅
python scripts/setup_kaggle.py  # Run setup first
```

**Setup Kaggle API:**
1. Get API credentials from https://www.kaggle.com/settings/account
2. Save to `~/.kaggle/kaggle.json` (use `setup_kaggle.py` for guidance)
3. Accept competition rules: https://www.kaggle.com/c/instacart-market-basket-analysis/rules

**Usage:**
```bash
python scripts/download_kaggle_dataset.py
```

**Output:** Raw CSV files in `data/raw/instacart/` (~1.3GB, 6 files)  
**Time:** 5-10 minutes

---

### 3. `explore_data_local.py` ⭐ NEW
Comprehensive local data exploration and quality validation.

**Prerequisites:**
```bash
pip install pandas numpy tabulate  # Already installed ✅
python scripts/download_kaggle_dataset.py  # Download data first
```

**Usage:**
```bash
python scripts/explore_data_local.py
```

**What it analyzes:**
- ✅ File validation (sizes, row counts)
- 📊 Reference tables (departments, aisles)
- 📦 Products hierarchy and distribution
- 🛍️ Order patterns (time, user behavior)
- 🛒 Top products and reorder rates
- 📋 Data quality summary

**Output:** Comprehensive report with insights and next steps  
**Time:** 2-3 minutes

---

## ☁️ Cloud Deployment (Require GCS Setup)

### 4. `upload_to_gcs.py`
Upload CSV files from local to GCS raw layer.

**Prerequisites:**
- GCS bucket created (via Terraform)
- Service account key with Storage Object Admin role
- `GOOGLE_APPLICATION_CREDENTIALS` env variable set

**Usage:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
python scripts/upload_to_gcs.py
```

### 5. `validate_iceberg_tables.py`
Validate Iceberg tables exist and have data.

**Usage:**
```bash
# Validate Bronze layer
python scripts/validate_iceberg_tables.py --layer bronze

# Validate Silver layer
python scripts/validate_iceberg_tables.py --layer silver
```

---

## 📋 Typical Workflow

### Phase 1: Local Setup (No GCS) ⭐ DO THIS FIRST
```bash
# Step 1: Setup Kaggle
python scripts/setup_kaggle.py

# Step 2: Download dataset
python scripts/download_kaggle_dataset.py

# Step 3: Explore locally
python scripts/explore_data_local.py
```

### Phase 2: Cloud Deployment (After GCS Setup)
```bash
# Step 4: Upload to GCS
python scripts/upload_to_gcs.py

# Step 5: Run Bronze ingestion (on Databricks)
# See: databricks/README.md for execution instructions

# Step 6: Validate Bronze
python scripts/validate_iceberg_tables.py --layer bronze

# Step 7: Run Silver transformation (on Databricks)
# See: databricks/README.md

# Step 8: Validate Silver
python scripts/validate_iceberg_tables.py --layer silver

# Step 9: Run data quality checks (on Databricks)
# See: databricks/README.md

# Step 10: Run dbt (build dimensional model)
cd etl/dbt_project
dbt run --profiles-dir . --target glue
dbt test --profiles-dir . --target glue
```

---

## 💡 Tips

- **Start local first:** Run Phase 1 to understand data before cloud deployment
- **Databricks execution:** PySpark scripts run on Databricks, not locally (see databricks/README.md)
- **Validation scripts:** Can run locally with proper GCS credentials
- **Incremental development:** Test each layer before proceeding to next

---

## 📚 Related Documentation

- [QUICK_START_LOCAL.md](../QUICK_START_LOCAL.md) - Step-by-step local setup guide
- [LOCAL_SETUP_CHECKLIST.md](../LOCAL_SETUP_CHECKLIST.md) - Progress tracking
- [databricks/README.md](../databricks/README.md) - Databricks execution guide
- [SETUP_GUIDE.md](../SETUP_GUIDE.md) - Full GCS + BigQuery setup

