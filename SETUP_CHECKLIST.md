# ✅ Setup Checklist - Instacart Lakehouse

**Complete setup in 7 days, $0 cost**

---

## 📋 Prerequisites (What You Need to Do)

### **Day 0: Account Creation (30 minutes)**

- [ ] **AWS Account**
  - Sign up: https://aws.amazon.com/
  - Enable free tier
  - Note: Access Key ID + Secret Access Key

- [ ] **Databricks AWS Trial**
  - Go to AWS Marketplace → Search "Databricks"
  - Subscribe to Databricks (14-day trial)
  - Create workspace (takes 10-15 min)
  - Create personal access token
  - Note: Host URL + Token

- [ ] **MongoDB Atlas** (Free Forever)
  - Sign up: https://www.mongodb.com/cloud/atlas/register
  - Create M0 FREE cluster (512MB)
  - Whitelist IP: `0.0.0.0/0` (allow all)
  - Create database user
  - Get connection string
  - Note: Connection URI

- [ ] **Kaggle API Token**
  - Login to kaggle.com
  - Go to Account → API → Create New Token
  - Downloads `kaggle.json`
  - Note: Keep this file safe

---

## 🚀 Day 1: Local Setup (30 minutes)

### **Step 1: Clone Repository**
```bash
git clone <repo-url>
cd Spark-Iceberg-DuckDB-Lakehouse
```

### **Step 2: Install Python Dependencies**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### **Step 3: Configure Environment**
```bash
# Copy template
cp .env.example .env

# Edit .env with your credentials from Day 0
```

**.env Configuration:**
```bash
# AWS
AWS_ACCESS_KEY_ID=<your-aws-key>
AWS_SECRET_ACCESS_KEY=<your-aws-secret>
AWS_REGION=us-east-1
S3_BUCKET=instacart-lakehouse-<your-unique-suffix>

# Databricks
DATABRICKS_HOST=https://<workspace-id>.cloud.databricks.com
DATABRICKS_TOKEN=<your-token>
DATABRICKS_CLUSTER_ID=<cluster-id>

# MongoDB
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/
MONGODB_DATABASE=instacart_metadata

# Paths
S3_RAW_PATH=s3://instacart-lakehouse-<suffix>/raw
S3_BRONZE_PATH=s3://instacart-lakehouse-<suffix>/bronze
S3_SILVER_PATH=s3://instacart-lakehouse-<suffix>/silver
S3_GOLD_PATH=s3://instacart-lakehouse-<suffix>/gold

# Local
LOCAL_DATA_PATH=./data
```

### **Step 4: Setup Kaggle**
```bash
# Linux/Mac
mkdir ~/.kaggle
cp /path/to/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

# Windows
mkdir %USERPROFILE%\.kaggle
copy kaggle.json %USERPROFILE%\.kaggle\
```

### **Step 5: Provision AWS Infrastructure**
```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve

# Note the S3 bucket name from output
terraform output s3_bucket_name
```

✅ **Day 1 Complete!** Infrastructure ready.

---

## 📊 Day 2: Data Acquisition (1-2 hours)

### **Step 1: Download Instacart Dataset**
```bash
python scripts/download_kaggle_dataset.py
```

Expected output:
```
Downloading instacart-market-basket-analysis...
✓ aisles.csv
✓ departments.csv
✓ products.csv
✓ orders.csv
✓ order_products__prior.csv
✓ order_products__train.csv
Complete! (~1.3GB in data/raw/instacart/)
```

### **Step 2: Upload to S3**
```bash
python scripts/upload_to_s3.py
```

Expected output:
```
Uploading to s3://instacart-lakehouse-xxx/raw/instacart/...
✓ aisles.csv (45KB)
✓ departments.csv (270B)
✓ products.csv (2.1MB)
✓ orders.csv (104MB)
✓ order_products__prior.csv (551MB)
✓ order_products__train.csv (38MB)
Upload complete! (~700MB)
```

### **Step 3: Verify Upload**
```bash
aws s3 ls s3://instacart-lakehouse-xxx/raw/instacart/ --recursive --human-readable
```

✅ **Day 2 Complete!** Data in S3.

---

## ⚙️ Day 3-4: Bronze & Silver Layers (4-6 hours)

### **Prerequisites:**
- Databricks workspace ready
- Cluster running (m5.large, 1 node)
- PySpark code uploaded

### **Step 1: Upload PySpark Code to Databricks**

Via Databricks UI:
1. Workspace → Upload → Select `pyspark/` folder
2. Or create notebooks from `.py` files

### **Step 2: Install Libraries on Cluster**

Databricks UI → Cluster → Libraries → Install New:
- **PyPI**: `pyiceberg`
- **PyPI**: `boto3`

### **Step 3: Run Bronze Ingestion**

Create notebook from `pyspark/bronze_ingestion.py`:

```python
# At top of notebook, set credentials
spark.conf.set("spark.hadoop.fs.s3a.access.key", "<AWS_KEY>")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "<AWS_SECRET>")

# Run ingestion
%run pyspark/bronze_ingestion.py
```

Expected output:
```
✓ aisles → bronze.aisles (134 rows)
✓ departments → bronze.departments (21 rows)
✓ products → bronze.products (49,688 rows)
✓ orders → bronze.orders (3,421,083 rows)
✓ order_products__prior → bronze.order_products_prior (32,434,489 rows)
✓ order_products__train → bronze.order_products_train (1,384,617 rows)
Bronze layer complete!
```

### **Step 4: Run Silver Transformation**

```python
%run pyspark/silver_transformation.py
```

Expected output:
```
✓ orders_enriched → silver.orders_enriched (3.4M rows)
✓ products_enriched → silver.products_enriched (49K rows)
✓ order_products_enriched → silver.order_products_enriched (33M rows)
Silver layer complete!
```

### **Step 5: Run Data Quality Checks**

```python
%run pyspark/data_quality_checks.py
```

Expected output:
```
✅ All quality checks passed
  - No nulls in primary keys
  - All foreign keys valid
  - Reordered values in {0, 1}
```

### **Step 6: Export Notebooks (IMPORTANT!)**

Before trial ends:
- File → Export → HTML
- Save all notebooks locally

✅ **Day 3-4 Complete!** Bronze + Silver ready.

---

## 🏅 Day 5: Gold Layer (dbt) (3-4 hours)

### **Step 1: Install dbt-spark**
```bash
pip install dbt-spark
```

### **Step 2: Configure dbt Profile**

Create `~/.dbt/profiles.yml`:

```yaml
instacart:
  target: prod
  outputs:
    prod:
      type: spark
      method: thrift
      host: <databricks-host>
      port: 443
      token: <databricks-token>
      cluster: <cluster-id>
      schema: gold
      connect_retries: 3
      connect_timeout: 30
```

### **Step 3: Test dbt Connection**
```bash
cd dbt_instacart
dbt debug --profiles-dir ~/.dbt
```

Expected: `All checks passed!`

### **Step 4: Run dbt Models**
```bash
# Staging layer
dbt run --select staging --profiles-dir ~/.dbt --target prod

# Marts layer
dbt run --select marts --profiles-dir ~/.dbt --target prod
```

Expected output:
```
✓ stg_aisles
✓ stg_departments
✓ stg_products
✓ stg_orders
✓ stg_order_products
✓ dim_product
✓ dim_orders
✓ fct_order_products
✓ mart_product_reorder_rate
✓ mart_department_demand
Completed successfully (10 models)
```

### **Step 5: Run dbt Tests**
```bash
dbt test --profiles-dir ~/.dbt --target prod
```

### **Step 6: Generate Documentation**
```bash
dbt docs generate --profiles-dir ~/.dbt
dbt docs serve --port 8002
```

Open: http://localhost:8002

📸 **Screenshot the lineage graph!**

✅ **Day 5 Complete!** Gold layer ready.

---

## 🏢 Day 6: Warehouse Service (2-3 hours)

### **Step 1: Start Local Services**
```bash
# Start MongoDB
docker-compose up -d mongodb

# Verify running
docker-compose ps
```

### **Step 2: Register Metadata**
```bash
python scripts/register_metadata.py
```

Expected:
```
Registering dataset: gold.dim_product
Registering dataset: gold.dim_orders
Registering dataset: gold.fct_order_products
Registering dataset: gold.mart_product_reorder_rate
Registering dataset: gold.mart_department_demand
✅ 5 datasets registered
```

### **Step 3: Seed Metrics**
```bash
python scripts/seed_instacart_metrics.py
```

Expected:
```
✨ Created: product_reorder_rate
✨ Created: department_reorder_rate
... (15 metrics total)
✅ Seeding complete! 15 metrics
```

### **Step 4: Start Warehouse API**
```bash
cd warehouse
uvicorn main:app --reload --port 8000
```

### **Step 5: Test API**

Open browser: http://localhost:8000/docs

Or test via CLI:
```bash
# List datasets
curl http://localhost:8000/datasets

# List metrics
curl http://localhost:8000/metrics

# Execute metric
curl -X POST http://localhost:8000/metrics/product_reorder_rate/execute \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"min_orders": 100, "limit": 20}}'
```

### **Step 6: Test Python SDK**
```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# List metrics
metrics = client.list_metrics()
print(f"Found {len(metrics)} metrics")

# Execute metric
result = client.execute_metric("product_reorder_rate")
print(result['preview'])
```

📸 **Screenshot API docs and results!**

✅ **Day 6 Complete!** Warehouse API running.

---

## 📝 Day 7: Documentation & Portfolio (2-3 hours)

### **Step 1: Capture Screenshots**

- [ ] Databricks cluster dashboard
- [ ] S3 bucket structure
- [ ] dbt docs lineage graph
- [ ] Warehouse API Swagger docs
- [ ] Metrics execution results
- [ ] MongoDB collections (via Mongo Express)

### **Step 2: Export Databricks Notebooks**

- [ ] Export all notebooks as HTML
- [ ] Save cluster configuration
- [ ] Screenshot job run history

### **Step 3: Create Presentation**

10-slide deck:
1. Project overview
2. Architecture diagram
3. Data flow (Bronze→Silver→Gold)
4. Sample metrics & results
5. Technologies used
6. Key learnings
7. Challenges & solutions
8. Metrics Store pattern
9. Performance metrics
10. Future enhancements

### **Step 4: Git Commit**
```bash
git add .
git commit -m "feat: complete lakehouse with 15 metrics"
git push
```

### **Step 5: Update Documentation**

- [ ] README.md with project summary
- [ ] Add screenshots to `docs/` folder
- [ ] Create demo video (optional)

✅ **Day 7 Complete!** Portfolio ready.

---

## 🔄 Post-Trial Maintenance

### **What Keeps Running (FREE):**
- ✅ S3 data (within 5GB free tier)
- ✅ MongoDB Atlas M0 (free forever)
- ✅ Warehouse API (local)
- ✅ DuckDB queries (local)

### **What Stops:**
- ❌ Databricks cluster (trial ends)

### **What You Can Still Do:**
- ✅ Query Gold data via DuckDB/API
- ✅ Execute metrics via API
- ✅ Show to recruiters
- ✅ Demo functionality

---

## ⚠️ Cost Monitoring

### **Daily Check:**
```bash
# Check S3 usage (should be ~2GB / 5GB free)
aws s3 ls s3://instacart-lakehouse-xxx/ --recursive --summarize | grep "Total Size"

# Check Databricks trial days remaining
# UI: Settings → Billing → Trial Information
```

### **Expected Costs:**
- AWS S3: $0 (within free tier)
- Databricks: $0 (trial)
- MongoDB: $0 (M0 forever free)
- **Total: $0.00** ✅

---

## 🆘 Troubleshooting

### **Issue: Databricks connection fails**
```bash
# Check credentials in .env
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN

# Test cluster is running
# Databricks UI → Compute → Check status
```

### **Issue: MongoDB connection refused**
```bash
# Restart MongoDB
docker-compose restart mongodb

# Check status
docker-compose logs mongodb
```

### **Issue: DuckDB can't read S3**
```bash
# Verify AWS credentials
aws s3 ls s3://instacart-lakehouse-xxx/

# Check .env file has correct credentials
```

### **Issue: dbt connection fails**
```bash
# Test dbt profile
cd dbt_instacart
dbt debug --profiles-dir ~/.dbt

# Verify Databricks cluster running
```

---

## ✅ Final Checklist

**Infrastructure:**
- [ ] AWS S3 bucket created
- [ ] Databricks cluster running
- [ ] MongoDB Atlas M0 cluster created

**Data:**
- [ ] Raw data in S3
- [ ] Bronze tables created (6 tables)
- [ ] Silver tables created (3 tables)
- [ ] Gold tables created (5 tables)

**Warehouse:**
- [ ] MongoDB metadata registered
- [ ] 15 metrics seeded
- [ ] API running successfully
- [ ] SDK tests passing

**Portfolio:**
- [ ] All notebooks exported
- [ ] Screenshots captured
- [ ] Presentation created
- [ ] GitHub updated
- [ ] LinkedIn post drafted

---

## 🎓 Interview Prep

**30-second pitch:**

> "I built an end-to-end data lakehouse processing 33 million Instacart records through a Medallion architecture using Apache Iceberg on AWS S3.
>
> The unique part is the Metrics Store—I stored business logic as data in MongoDB rather than YAML files, enabling self-service analytics where analysts can register and execute metrics via API without code deployment.
>
> The entire platform runs on free tiers—AWS S3, Databricks trial, MongoDB Atlas—total cost zero dollars. It demonstrates both modern data engineering patterns and cost awareness."

**Key metrics to mention:**
- 33M+ records processed
- 15 reusable business metrics
- Sub-500ms query response time
- $0 total cost
- 7-day implementation

---

**Ready to start? Follow Day 1!** 🚀
