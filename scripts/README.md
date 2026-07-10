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
cd dbt_instacart
dbt run
dbt test
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

