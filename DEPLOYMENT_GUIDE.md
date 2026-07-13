# 🚀 DEPLOYMENT GUIDE

**Complete step-by-step guide to deploy Instacart Lakehouse**

---

## ⏱️ Time Required: 3-4 hours

- **Reading & Setup:** 30 min
- **AWS Infrastructure:** 1 hour
- **Data Pipeline:** 1-1.5 hours
- **ML & API:** 1 hour
- **Testing:** 30 min

---

## 📋 PRE-REQUISITES

### Required
- [ ] AWS account with admin access
- [ ] AWS CLI installed and configured
- [ ] Docker & Docker Compose installed
- [ ] Python 3.9+ with pip
- [ ] Terraform 1.0+
- [ ] Instacart dataset (6 CSV files from Kaggle)

### Nice to Have
- [ ] dbt CLI (for local testing)
- [ ] MongoDB Compass (for viewing recommendations)

---

## 🔍 PRE-FLIGHT CHECKS (10 minutes)

Run these before deploying to catch issues early:

```bash
# 1. Python syntax
python -m py_compile etl/glue_jobs/bronze_ingestion.py
python -m py_compile etl/glue_jobs/silver_transformation.py
python -m py_compile etl/ml/train_reorder_model.py
python -m py_compile warehouse/api/main.py

# 2. dbt parse
cd etl/dbt_project && dbt parse && dbt list

# 3. Terraform validate
cd terraform && terraform init && terraform validate

# 4. Docker validate
docker-compose config
```

**Expected:** All commands succeed ✅

---

## 🏗️ DEPLOYMENT STEPS

### **STEP 1: AWS Credentials (5 min)**

```bash
# Configure AWS CLI
aws configure
# Enter: Access Key, Secret Key, Region (us-east-1), Format (json)

# Verify
aws sts get-caller-identity
```

---

### **STEP 2: Terraform Setup (15 min)**

```bash
cd terraform/

# Create variables file
cp terraform.tfvars.example terraform.tfvars

# Edit with your values:
nano terraform.tfvars
```

**terraform.tfvars:**
```hcl
project_name    = "instacart-lakehouse"
environment     = "prod"
aws_region      = "us-east-1"
s3_bucket_name  = "instacart-lakehouse-<your-unique-suffix>"
```

**Deploy infrastructure:**
```bash
terraform plan    # Review changes
terraform apply   # Type 'yes' to confirm
```

**Verify:**
```bash
aws s3 ls  # Check bucket exists
aws glue get-database --name instacart_lakehouse_prod
aws glue list-jobs
```

---

### **STEP 3: Upload Data (15 min)**

```bash
# Sync CSVs to S3
aws s3 sync ./data/instacart/ s3://<your-bucket>/raw/instacart/

# Verify 6 files uploaded
aws s3 ls s3://<your-bucket>/raw/instacart/
```

**Expected files:**
- orders.csv
- products.csv
- aisles.csv
- departments.csv
- order_products__prior.csv
- order_products__train.csv

---

### **STEP 4: Bronze Layer (10-15 min)**

```bash
# Start Glue Job
aws glue start-job-run \
  --job-name instacart-lakehouse-bronze-ingestion

# Monitor (note the RunId from previous command)
aws glue get-job-run \
  --job-name instacart-lakehouse-bronze-ingestion \
  --run-id <run-id>
```

**Wait for:** `"JobRunState": "SUCCEEDED"`

**Verify via Athena:**
```sql
SELECT COUNT(*) FROM instacart_lakehouse.bronze.orders;
-- Expected: ~3.4M rows
```

---

### **STEP 5: Silver Layer (15-20 min)**

```bash
aws glue start-job-run \
  --job-name instacart-lakehouse-silver-transformation
```

Monitor same way as Bronze.

---

### **STEP 6: Gold Layer (dbt) (5-10 min)**

```bash
cd etl/dbt_project/

# Edit profiles.yml with your AWS account ID and role ARN
nano profiles.yml

# Test connection
dbt debug

# Run dbt
dbt run --target glue
dbt test --target glue
```

**Expected:** 10 models created successfully

---

### **STEP 7: Install Python Dependencies (2 min)**

```bash
pip install -r requirements.txt
```

---

### **STEP 8: Start MongoDB (1 min)**

```bash
docker-compose up -d mongodb

# Verify
docker-compose ps
```

---

### **STEP 9: Train ML Model (5-10 min)**

```bash
# Set environment variables
export AWS_ACCOUNT_ID=<your-account>
export AWS_REGION=us-east-1
export DUCKDB_ROLE_ARN=arn:aws:iam::<account>:role/DuckDBRole
export USE_GLUE_CATALOG=true
export MONGODB_URI=mongodb://admin:admin123@localhost:27017

# Train model
python etl/ml/train_reorder_model.py
```

**Expected output:**
```
📊 MODEL PERFORMANCE
AUC:       0.XXXX
F1 Score:  0.XXXX
✅ Model saved to etl/ml/model_artifacts/reorder_model.xgb
```

---

### **STEP 10: Generate Recommendations (10-15 min)**

```bash
python etl/ml/generate_recommendations.py
```

**Expected:**
```
✅ Created recommendations for XXX,XXX users
💾 Successfully wrote all recommendations
```

---

### **STEP 11: Start Warehouse API (1 min)**

```bash
docker-compose up -d warehouse-api

# Check logs
docker-compose logs -f warehouse-api
```

Wait for: `Uvicorn running on http://0.0.0.0:8000`

---

### **STEP 12: Test Endpoints (5 min)**

```bash
# Health check
curl http://localhost:8000/

# Query test
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM fct_order_products LIMIT 5"}'

# Recommendation test
curl http://localhost:8000/recommendations/12345

# Stats
curl http://localhost:8000/recommendations/stats
```

---

## ✅ SUCCESS CRITERIA

Deployment successful when:

- [ ] ✅ All Terraform resources created
- [ ] ✅ 6 Bronze tables in Glue Catalog
- [ ] ✅ 3 Silver tables in Glue Catalog  
- [ ] ✅ 10 Gold tables (dbt models)
- [ ] ✅ ML model trained (AUC > 0.75)
- [ ] ✅ Recommendations in MongoDB (200K+ users)
- [ ] ✅ API health check returns 200
- [ ] ✅ GET /recommendations/{user_id} returns top-10

---

## 🐛 TROUBLESHOOTING

### **Issue: Terraform fails with "bucket already exists"**
**Fix:** Change `s3_bucket_name` in terraform.tfvars to use unique suffix

### **Issue: Glue Job fails with "AccessDenied"**
**Fix:** Check IAM role has s3:GetObject, s3:PutObject permissions

### **Issue: dbt fails with "relation does not exist"**
**Fix:** Ensure Bronze/Silver Glue Jobs completed successfully first

### **Issue: ML training fails with "table not found"**
**Fix:** Run `dbt run` first to create Gold tables

### **Issue: DuckDB can't connect to Glue Catalog**
**Fix:** Check AWS credentials, verify USE_GLUE_CATALOG=true

### **Issue: No recommendations returned**
**Fix:** 
1. Check MongoDB: `docker-compose exec mongodb mongosh`
2. Verify collection: `use instacart_warehouse; db.recommendations.findOne()`

---

## 🔄 OPTIONAL: Airflow Setup

If you want full orchestration:

```bash
# Start Airflow
docker-compose up -d airflow

# Access UI: http://localhost:8080
# Username: admin, Password: admin

# Configure Airflow Variables:
# - s3_bucket: <your-bucket>
# - aws_region: us-east-1
# - project_root: /opt/airflow/dags

# Enable & trigger DAG
```

---

## 📊 RECORD YOUR METRICS

Document these for reference:

```
Pipeline Times:
- Bronze Ingestion:     _____ min
- Silver Transform:     _____ min
- dbt Gold:            _____ min
- ML Training:         _____ min
- Recommendation Gen:  _____ min
Total:                 _____ min

Model Metrics:
- AUC:        _____
- F1:         _____
- Precision:  _____
- Recall:     _____

Data:
- Users:              _____
- Products:           _____
- Training Samples:   _____
- Recommendations:    _____
```

---

## 📚 NEXT STEPS

After successful deployment:

1. **Document actual metrics** in `docs/ML_MODEL_NOTES.md`
2. **Test with real queries** via API
3. **Set up monitoring** (CloudWatch, Grafana)
4. **Schedule Airflow DAG** (if using)
5. **Present to stakeholders**

---

## 🆘 NEED HELP?

Check these resources:
- **REFACTOR_BLUEPRINT.md** - Technical architecture
- **DEVELOPMENT.md** - Code structure & testing
- **docs/archive/** - Detailed legacy docs

Or review:
- AWS Glue Job logs in CloudWatch
- Docker logs: `docker-compose logs <service>`
- dbt logs: `logs/dbt.log`

---

**🎉 Congratulations on deploying your data lakehouse!**
