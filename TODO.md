# TODO: Complete Deployment Checklist

**Goal:** Deploy the entire Instacart data lakehouse pipeline from scratch

**Estimated Time:** 6-8 hours (first time), 2-3 hours (subsequent runs)

---

## 📋 Phase 1: Local Environment Setup (30 minutes)

### 1.1 Python Environment
- [ ] Install Python 3.9+ (`python --version`)
- [ ] Create virtual environment: `python -m venv venv`
- [ ] Activate venv:
  - Windows: `venv\Scripts\activate`
  - Linux/Mac: `source venv/bin/activate`
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Verify installations:
  ```bash
  python -c "import pyspark; print(pyspark.__version__)"
  python -c "import duckdb; print(duckdb.__version__)"
  python -c "import pymongo; print(pymongo.__version__)"
  ```

### 1.2 Install Required Tools
- [ ] Install AWS CLI: https://aws.amazon.com/cli/
- [ ] Verify: `aws --version`
- [ ] Install Terraform: https://www.terraform.io/downloads
- [ ] Verify: `terraform --version`
- [ ] Install Databricks CLI: `pip install databricks-cli`
- [ ] Verify: `databricks --version`
- [ ] Install dbt-spark: `pip install dbt-spark`
- [ ] Verify: `dbt --version`

### 1.3 Code Verification
- [ ] Clone/pull latest code
- [ ] Check structure: `tree -L 2` or `dir /s` (Windows)
- [ ] Verify all modules exist:
  - `config/instacart_config.py`
  - `pyspark/bronze_ingestion.py`
  - `pyspark/silver_transformation.py`
  - `warehouse/main.py`
  - `dbt_instacart/profiles.yml`
  - `terraform/main.tf`

---

## 📋 Phase 2: AWS Setup (45 minutes)

### 2.1 AWS Account Setup
- [ ] Create AWS account (if not exists): https://aws.amazon.com/free/
- [ ] Sign in to AWS Console
- [ ] Navigate to IAM → Users → Create User
- [ ] User name: `instacart-pipeline-user`
- [ ] Attach policies:
  - `AmazonS3FullAccess` (or create custom policy)
- [ ] Create Access Key (Access Key ID + Secret)
- [ ] **SAVE CREDENTIALS SECURELY**

### 2.2 Configure AWS CLI
- [ ] Run: `aws configure`
- [ ] Enter:
  ```
  AWS Access Key ID: [your_key]
  AWS Secret Access Key: [your_secret]
  Default region: us-east-1
  Default output format: json
  ```
- [ ] Test: `aws s3 ls` (should show no errors)

### 2.3 Deploy Infrastructure with Terraform
- [ ] Navigate to terraform directory: `cd terraform`
- [ ] Initialize: `terraform init`
- [ ] Preview changes: `terraform plan`
- [ ] Deploy: `terraform apply -auto-approve`
- [ ] **SAVE OUTPUT VALUES:**
  ```bash
  terraform output s3_bucket_name         # Save this!
  terraform output aws_access_key_id      # Save this!
  terraform output aws_secret_access_key  # Save this!
  ```
- [ ] Verify S3 bucket created: `aws s3 ls` (should see your bucket)

### 2.4 Test S3 Access
- [ ] Create test file: `echo "test" > test.txt`
- [ ] Upload: `aws s3 cp test.txt s3://[your-bucket-name]/test/`
- [ ] List: `aws s3 ls s3://[your-bucket-name]/test/`
- [ ] Delete: `aws s3 rm s3://[your-bucket-name]/test/test.txt`
- [ ] Remove local file: `rm test.txt` (or `del test.txt` on Windows)

---

## 📋 Phase 3: Databricks Setup (30 minutes)

### 3.1 Create Databricks Account
- [ ] Go to: AWS Marketplace → search "Databricks" → Start trial (14-day)
- [ ] Click "Subscribe" / "Launch" (Databricks on AWS, NOT Community Edition)
- [ ] Complete registration
- [ ] Verify email
- [ ] Sign in to workspace

### 3.2 Create Cluster
- [ ] Click "Compute" in sidebar
- [ ] Click "Create Cluster"
- [ ] Configuration:
  - **Cluster name:** `instacart-lakehouse`
  - **Cluster mode:** Single Node
  - **Databricks Runtime:** 13.3 LTS (or latest LTS)
  - **Node type:** Default (Community Edition has 1 option)
  - **Terminate after:** 120 minutes of inactivity
- [ ] Click "Create Cluster"
- [ ] Wait for cluster to start (2-3 minutes)
- [ ] **SAVE CLUSTER ID** (from URL or cluster details)

### 3.3 Install Libraries on Cluster
- [ ] Go to cluster page → "Libraries" tab
- [ ] Click "Install New"
- [ ] Select "PyPI"
- [ ] Install these packages ONE BY ONE:
  - [ ] `pyiceberg`
  - [ ] `boto3`
  - [ ] `pymongo`
- [ ] Wait for each to install (Status: "Installed")

### 3.4 Generate Access Token
- [ ] Click your email (top right) → "User Settings"
- [ ] Go to "Access Tokens" tab
- [ ] Click "Generate New Token"
- [ ] Comment: `instacart-pipeline`
- [ ] Lifetime: 90 days (or as needed)
- [ ] Click "Generate"
- [ ] **SAVE TOKEN SECURELY** (you won't see it again!)

### 3.5 Configure Databricks CLI
- [ ] Run: `databricks configure --token`
- [ ] Enter:
  ```
  Databricks Host: https://<workspace>.cloud.databricks.com
  Token: [paste your token]
  ```
- [ ] Test: `databricks clusters list` (should show your cluster)

### 3.6 Configure AWS Credentials in Databricks
- [ ] Create secrets scope: 
  ```bash
  databricks secrets create-scope --scope instacart-prod
  ```
- [ ] Add AWS credentials:
  ```bash
  # This will open editor - paste your key and save
  databricks secrets put --scope instacart-prod --key aws_access_key_id
  databricks secrets put --scope instacart-prod --key aws_secret_access_key
  ```
- [ ] Verify: `databricks secrets list --scope instacart-prod`

---

## 📋 Phase 4: MongoDB Setup (10 minutes)

**Có 2 options: Atlas (cloud) hoặc Docker (local)**

### Option A: Docker Local (Khuyên dùng - Dễ nhất) ✅

- [ ] Đảm bảo Docker Desktop đang chạy
- [ ] Copy .env: `cp .env.example .env` (Windows: `copy .env.example .env`)
- [ ] Edit .env với AWS credentials
- [ ] Start services: `docker-compose up -d`
- [ ] Wait 30 seconds
- [ ] Check status: `docker-compose ps`
- [ ] Test MongoDB:
  ```bash
  docker-compose exec mongodb mongosh -u admin -p admin123 --eval "db.adminCommand('ping')"
  ```
- [ ] Test API: Open browser → http://localhost:8000/docs
- [ ] MongoDB connection string: `mongodb://admin:admin123@localhost:27017/`

**Done! MongoDB + API running in Docker** 🎯

### Option B: MongoDB Atlas (Cloud - Also Good)
- [ ] Go to: https://www.mongodb.com/cloud/atlas/register
- [ ] Sign up for free account
- [ ] Create organization: `Personal`
- [ ] Create project: `instacart-lakehouse`
- [ ] Click "Build a Database"
- [ ] Choose: **FREE (M0) tier**
- [ ] Provider: **AWS**
- [ ] Region: **us-east-1** (same as S3)
- [ ] Cluster name: `instacart-metadata`
- [ ] Click "Create"

### 4.2 Configure MongoDB Access
- [ ] **Security → Database Access**
  - [ ] Add user: `instacart_user`
  - [ ] Password: Generate secure password
  - [ ] **SAVE PASSWORD**
  - [ ] Role: Atlas admin (or Read/Write to any database)
- [ ] **Security → Network Access**
  - [ ] Click "Add IP Address"
  - [ ] Click "Allow Access from Anywhere" (0.0.0.0/0)
  - [ ] Confirm (for development only)
  
### 4.3 Get Connection String
- [ ] Go to "Database" → Click "Connect"
- [ ] Choose "Connect your application"
- [ ] Driver: Python, Version: 3.12 or later
- [ ] Copy connection string
- [ ] **SAVE CONNECTION STRING:**
  ```
  mongodb+srv://instacart_user:[password]@instacart-metadata.xxxxx.mongodb.net/?retryWrites=true&w=majority
  ```
- [ ] Replace `[password]` with your actual password
- [ ] Test connection:
  ```python
  python -c "from pymongo import MongoClient; print(MongoClient('your_connection_string').server_info())"
  ```

---

## 📋 Phase 5: Environment Configuration (15 minutes)

### 5.1 Create .env File
- [ ] Navigate to project root
- [ ] Copy template: `cp .env.example .env` (or `copy .env.example .env` on Windows)
- [ ] Edit `.env` with your actual values:
  ```bash
  # AWS
  AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXX
  AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  AWS_REGION=us-east-1
  S3_BUCKET=instacart-lakehouse-xxxx  # From terraform output
  
  # Databricks
  DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
  DATABRICKS_TOKEN=dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  DATABRICKS_CLUSTER_ID=xxxx-xxxxxx-xxxxxxx
  
  # MongoDB
  MONGODB_URI=mongodb+srv://instacart_user:password@cluster.xxxxx.mongodb.net/
  MONGODB_DATABASE=instacart_metadata
  
  # Paths
  S3_RAW_PATH=s3a://instacart-lakehouse-xxxx/raw/instacart
  S3_BRONZE_PATH=s3a://instacart-lakehouse-xxxx/bronze
  S3_SILVER_PATH=s3a://instacart-lakehouse-xxxx/silver
  S3_GOLD_PATH=s3a://instacart-lakehouse-xxxx/gold
  ```
- [ ] **Verify file is in .gitignore** (NEVER commit .env!)

### 5.2 Test Configuration
- [ ] Run config test:
  ```bash
  python config/instacart_config.py
  ```
- [ ] Should output configuration summary without errors

### 5.3 Setup dbt Profile
- [ ] Create dbt config directory:
  ```bash
  mkdir -p ~/.dbt  # Linux/Mac
  mkdir %USERPROFILE%\.dbt  # Windows
  ```
- [ ] Copy profiles:
  ```bash
  cp dbt_instacart/profiles.yml ~/.dbt/profiles.yml
  ```
- [ ] Edit `~/.dbt/profiles.yml` with your Databricks credentials
- [ ] Test dbt connection:
  ```bash
  cd dbt_instacart
  dbt debug --profiles-dir ~/.dbt
  ```

---

## 📋 Phase 6: Data Acquisition (30 minutes)

### 6.1 Setup Kaggle API
- [ ] Go to: https://www.kaggle.com/ (create account if needed)
- [ ] Go to: https://www.kaggle.com/settings/account
- [ ] Scroll to "API" section
- [ ] Click "Create New Token"
- [ ] Download `kaggle.json`
- [ ] Place file:
  - Linux/Mac: `~/.kaggle/kaggle.json`
  - Windows: `C:\Users\[YourName]\.kaggle\kaggle.json`
- [ ] Set permissions (Linux/Mac): `chmod 600 ~/.kaggle/kaggle.json`
- [ ] Test: `kaggle datasets list`

### 6.2 Download Instacart Dataset
- [ ] Navigate to project root
- [ ] Run download script:
  ```bash
  python scripts/download_kaggle_dataset.py
  ```
- [ ] Wait for download (~1.3GB, takes 5-15 minutes)
- [ ] Verify files in `data/raw/instacart/`:
  - [ ] `orders.csv`
  - [ ] `order_products__prior.csv`
  - [ ] `order_products__train.csv`
  - [ ] `products.csv`
  - [ ] `aisles.csv`
  - [ ] `departments.csv`

### 6.3 Upload Data to S3
- [ ] Run upload script:
  ```bash
  python scripts/upload_to_s3.py
  ```
- [ ] Wait for upload (5-10 minutes)
- [ ] Verify upload:
  ```bash
  aws s3 ls s3://[your-bucket]/raw/instacart/ --recursive
  ```
- [ ] Should see 6 CSV files

---

## 📋 Phase 7: Pipeline Execution (2-3 hours)

### 7.1 Bronze Layer Ingestion

**Option A: Run Locally (Quick Test)**
- [ ] Test on small dataset locally:
  ```bash
  # Requires local Spark installation
  spark-submit --master local[*] --driver-memory 4g pyspark/bronze_ingestion.py
  ```

**Option B: Run on Databricks (Recommended)**
- [ ] Upload code to DBFS:
  ```bash
  # Create zip
  zip -r pipeline.zip pyspark/ config/
  
  # Upload
  databricks fs cp pipeline.zip dbfs:/jobs/instacart_pipeline.zip --overwrite
  
  # Extract
  databricks fs unzip dbfs:/jobs/instacart_pipeline.zip dbfs:/jobs/instacart_pipeline/
  ```
- [ ] Create job config file `databricks_jobs/bronze_job.json`:
  ```json
  {
    "name": "Bronze Ingestion",
    "existing_cluster_id": "YOUR_CLUSTER_ID",
    "spark_python_task": {
      "python_file": "dbfs:/jobs/instacart_pipeline/pyspark/bronze_ingestion.py"
    },
    "libraries": [
      {"pypi": {"package": "pyiceberg"}},
      {"pypi": {"package": "boto3"}}
    ]
  }
  ```
- [ ] Create job:
  ```bash
  databricks jobs create --json-file databricks_jobs/bronze_job.json
  ```
- [ ] Note the job ID from output
- [ ] Run job:
  ```bash
  databricks jobs run-now --job-id [job-id]
  ```
- [ ] Monitor: Go to Databricks UI → Workflows → Runs
- [ ] Wait for completion (~10-15 minutes)
- [ ] Check logs for errors
- [ ] Verify Bronze tables created:
  ```bash
  aws s3 ls s3://[your-bucket]/bronze/ --recursive
  ```

### 7.2 Silver Layer Transformation
- [ ] Create job config: `databricks_jobs/silver_job.json` (similar to bronze)
- [ ] Create job:
  ```bash
  databricks jobs create --json-file databricks_jobs/silver_job.json
  ```
- [ ] Run job:
  ```bash
  databricks jobs run-now --job-id [job-id]
  ```
- [ ] Monitor in Databricks UI (~10-15 minutes)
- [ ] Verify Silver tables:
  ```bash
  aws s3 ls s3://[your-bucket]/silver/ --recursive
  ```

### 7.3 Data Quality Checks
- [ ] Create job for data quality checks
- [ ] Run:
  ```bash
  databricks jobs run-now --job-id [job-id]
  ```
- [ ] Review quality report in logs

### 7.4 Gold Layer with dbt
- [ ] Test dbt locally first:
  ```bash
  cd dbt_instacart
  dbt debug --profiles-dir ~/.dbt
  dbt compile --profiles-dir ~/.dbt
  ```
- [ ] Run dbt on Databricks:
  ```bash
  dbt run --profiles-dir ~/.dbt --target prod
  ```
- [ ] Run dbt tests:
  ```bash
  dbt test --profiles-dir ~/.dbt --target prod
  ```
- [ ] Generate documentation:
  ```bash
  dbt docs generate --profiles-dir ~/.dbt
  dbt docs serve
  ```
- [ ] Verify Gold tables:
  ```bash
  aws s3 ls s3://[your-bucket]/gold/ --recursive
  ```

### 7.5 Register Metadata to MongoDB
- [ ] Run metadata registration:
  ```bash
  python scripts/register_metadata.py
  ```
- [ ] Verify in MongoDB Atlas:
  - Go to Database → Browse Collections
  - Check `instacart_metadata` database
  - Should see `datasets` collection with 4 documents

---

## 📋 Phase 8: Warehouse Service Deployment (30 minutes)

### 8.1 Start MongoDB Locally (Alternative to Atlas)
- [ ] If using local MongoDB:
  ```bash
  docker run -d -p 27017:27017 --name mongodb mongo:latest
  ```
- [ ] Update MONGODB_URI in .env to `mongodb://localhost:27017`

### 8.2 Test Warehouse Components
- [ ] Test DuckDB engine:
  ```python
  from warehouse.engine import DuckDBEngine
  engine = DuckDBEngine()
  print("DuckDB initialized successfully")
  ```
- [ ] Test MongoDB metadata:
  ```python
  from warehouse.metadata import MetadataStore
  store = MetadataStore()
  datasets = store.list_datasets()
  print(f"Found {len(datasets)} datasets")
  ```

### 8.3 Start Warehouse API
- [ ] Navigate to warehouse directory:
  ```bash
  cd warehouse
  ```
- [ ] Start server:
  ```bash
  uvicorn main:app --reload --port 8000
  ```
- [ ] Server should start at http://localhost:8000
- [ ] Open browser: http://localhost:8000/docs (should see Swagger UI)

### 8.4 Test API Endpoints
- [ ] Test health:
  ```bash
  curl http://localhost:8000/
  ```
- [ ] Test list datasets:
  ```bash
  curl http://localhost:8000/datasets
  ```
- [ ] Test get dataset:
  ```bash
  curl http://localhost:8000/datasets/gold.dim_product
  ```
- [ ] Test query:
  ```bash
  curl -X POST http://localhost:8000/query \
    -H "Content-Type: application/json" \
    -d '{"sql": "SELECT COUNT(*) FROM gold.fct_order_products"}'
  ```

### 8.5 Test Python SDK
- [ ] Create test script `test_sdk.py`:
  ```python
  from warehouse.sdk import WarehouseClient
  
  client = WarehouseClient("http://localhost:8000")
  
  # List datasets
  datasets = client.list_datasets()
  print(f"Datasets: {len(datasets)}")
  
  # Query
  df = client.query("SELECT * FROM gold.dim_product LIMIT 10")
  print(df)
  
  client.close()
  ```
- [ ] Run: `python test_sdk.py`
- [ ] Should print 10 rows

---

## 📋 Phase 9: Airflow Setup (Optional - 1 hour)

### 9.1 Install Airflow
- [ ] Create Airflow directory: `mkdir ~/airflow`
- [ ] Install:
  ```bash
  pip install apache-airflow==2.7.0
  ```
- [ ] Initialize:
  ```bash
  airflow db init
  ```
- [ ] Create user:
  ```bash
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
  ```

### 9.2 Configure Airflow
- [ ] Set Airflow home:
  ```bash
  export AIRFLOW_HOME=~/airflow  # Add to ~/.bashrc
  ```
- [ ] Copy DAG:
  ```bash
  cp dags/instacart_pipeline_dag.py ~/airflow/dags/
  ```
- [ ] Set Airflow variables:
  ```bash
  airflow variables set s3_bucket "instacart-lakehouse-xxxx"
  airflow variables set project_root "/path/to/project"
  ```

### 9.3 Start Airflow
- [ ] Start webserver (Terminal 1):
  ```bash
  airflow webserver --port 8080
  ```
- [ ] Start scheduler (Terminal 2):
  ```bash
  airflow scheduler
  ```
- [ ] Open browser: http://localhost:8080
- [ ] Login with admin credentials
- [ ] Enable DAG: `instacart_lakehouse_pipeline`
- [ ] Trigger manually to test

---

## 📋 Phase 10: Validation & Testing (30 minutes)

### 10.1 Data Validation
- [ ] Check row counts match expected:
  ```python
  from warehouse.sdk import WarehouseClient
  client = WarehouseClient()
  
  # Expected counts
  print("Orders:", client.query("SELECT COUNT(*) FROM gold.dim_orders"))
  print("Products:", client.query("SELECT COUNT(*) FROM gold.dim_product"))
  ```

### 10.2 Run Sample Queries
- [ ] Top 10 products:
  ```sql
  SELECT product_name, total_orders
  FROM gold.dim_product
  ORDER BY total_orders DESC
  LIMIT 10
  ```
- [ ] Reorder rate by product:
  ```sql
  SELECT product_name, reorder_rate
  FROM gold.mart_product_reorder_rate
  ORDER BY reorder_rate DESC
  LIMIT 10
  ```
- [ ] Orders by day of week:
  ```sql
  SELECT order_dow, COUNT(*) as orders
  FROM gold.fct_order_products
  GROUP BY order_dow
  ORDER BY order_dow
  ```

### 10.3 Check Data Quality
- [ ] No nulls in key columns
- [ ] Foreign keys are valid
- [ ] Date ranges are reasonable
- [ ] Counts match across layers

---

## 📋 Phase 11: Documentation & Cleanup (20 minutes)

### 11.1 Document Your Setup
- [ ] Create `MY_SETUP.md` with:
  - AWS account details
  - S3 bucket name
  - Databricks cluster ID
  - MongoDB connection string
  - Any issues encountered
  - Solutions applied

### 11.2 Cleanup (After Testing)
- [ ] Stop Airflow (Ctrl+C in both terminals)
- [ ] Stop warehouse API (Ctrl+C)
- [ ] Terminate Databricks cluster (if not using)
- [ ] Remove test files from S3:
  ```bash
  # Optional: aws s3 rm s3://[bucket]/test/ --recursive
  ```

### 11.3 Cost Monitoring
- [ ] Check AWS billing dashboard
- [ ] Check MongoDB Atlas usage
- [ ] Should be ~$0.05-0.10 for testing

---

## 📋 Phase 12: Production Readiness (Optional)

### 12.1 Security Hardening
- [ ] Rotate AWS keys
- [ ] Restrict MongoDB network access
- [ ] Use IAM roles instead of keys (if deploying to EC2)
- [ ] Enable S3 bucket versioning
- [ ] Enable CloudWatch logging

### 12.2 CI/CD Setup
- [ ] Create GitHub Actions workflow
- [ ] Automated testing on push
- [ ] Automated deployment to Databricks

### 12.3 Monitoring
- [ ] Setup CloudWatch alarms
- [ ] Setup Databricks job alerts
- [ ] Setup MongoDB alerts

---

## ✅ Success Criteria

You've successfully deployed when:

- [ ] ✅ All 6 CSV files uploaded to S3 raw layer
- [ ] ✅ Bronze layer has 6 Iceberg tables
- [ ] ✅ Silver layer has 4 Iceberg tables
- [ ] ✅ Gold layer has 4 tables (3 dimensions + 1 fact)
- [ ] ✅ MongoDB has metadata for 4 Gold tables
- [ ] ✅ Warehouse API responds on http://localhost:8000
- [ ] ✅ Can query via Python SDK and get DataFrames
- [ ] ✅ No errors in Databricks job logs
- [ ] ✅ dbt tests pass 100%

---

## 🆘 Troubleshooting Guide

### Issue: AWS credentials not working
```bash
# Verify credentials
aws sts get-caller-identity

# Reconfigure
aws configure
```

### Issue: Databricks cluster won't start
- Check if cluster was terminated
- Restart cluster in UI
- Wait 2-3 minutes

### Issue: S3 access denied from Databricks
- Verify Databricks secrets are set correctly
- Check IAM policy allows S3 access

### Issue: dbt connection fails
- Verify cluster is running
- Check token hasn't expired
- Run `dbt debug --profiles-dir ~/.dbt`

### Issue: MongoDB connection fails
- Check connection string format
- Verify password has no special characters needing URL encoding
- Check IP whitelist includes your IP

### Issue: DuckDB can't read Iceberg
- Verify DuckDB Iceberg extension is installed
- Check S3 credentials are available
- Verify Iceberg tables exist in S3

---

## 📊 Time Estimate Summary

| Phase | Time | Cumulative |
|-------|------|------------|
| Local Setup | 30 min | 30 min |
| AWS Setup | 45 min | 1h 15min |
| Databricks Setup | 30 min | 1h 45min |
| MongoDB Setup | 20 min | 2h 05min |
| Configuration | 15 min | 2h 20min |
| Data Acquisition | 30 min | 2h 50min |
| Pipeline Execution | 2-3 hours | 5h 50min |
| Warehouse Deployment | 30 min | 6h 20min |
| Airflow (Optional) | 1 hour | 7h 20min |
| Validation | 30 min | 7h 50min |
| Documentation | 20 min | 8h 10min |

**First-time total: 6-8 hours**  
**Subsequent runs: 2-3 hours**

---

## 💡 Tips for Success

1. **Do phases in order** - each builds on the previous
2. **Save all credentials immediately** - you may not see them again
3. **Test each phase** before moving to next
4. **Take breaks** - don't rush through 8 hours straight
5. **Document issues** - helps debugging later
6. **Use community forums** - AWS, Databricks, MongoDB all have active communities
7. **Start small** - test with subset of data first

---

## 📚 Helpful Resources

- AWS S3 Console: https://s3.console.aws.amazon.com/
- Databricks on AWS: https://aws.amazon.com/marketplace/serverless/amazon-databricks
- MongoDB Atlas: https://cloud.mongodb.com/
- Kaggle API Docs: https://www.kaggle.com/docs/api
- dbt Docs: https://docs.getdbt.com/
- FastAPI Docs: https://fastapi.tiangolo.com/

---

**Good luck! 🚀 Remember: First time takes longest. You'll learn the most valuable lessons from troubleshooting issues!**
