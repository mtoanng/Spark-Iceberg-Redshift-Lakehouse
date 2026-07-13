# 🏗️ Instacart Lakehouse - Project Master Document

**Complete project context, implementation status, and execution plan**

---

## 📋 Table of Contents

1. [What We're Building](#what-were-building)
2. [Architecture](#architecture)
3. [Implementation Status](#implementation-status)
4. [Metrics Store Feature](#metrics-store-feature)
5. [Execution Plan (Free Tier)](#execution-plan-free-tier)
6. [Next Steps](#next-steps)

---

## 🎯 What We're Building

### **Project Goal**
End-to-end data lakehouse processing **Instacart e-commerce data** (33M+ records) with:
- **Medallion architecture** (Bronze → Silver → Gold)
- **Apache Iceberg** tables on S3
- **dbt** for dimensional modeling
- **MongoDB** for metadata catalog + business metrics definitions
- **DuckDB** for fast analytical queries
- **FastAPI** warehouse service with REST API
- **Python SDK** for easy access

### **Dataset: Instacart Market Basket (Kaggle)**
- 33M+ order records
- 6 CSV files: orders, products, aisles, departments, order_products_prior, order_products_train
- **NO price/revenue data** - only purchase behavior (reorders, cart sequence)
- Focus: Market basket analysis, reorder patterns, demand by time/department

**Key Analytics:**
- Reorder rate by product/department/user
- Basket size by hour/day of week
- Product affinity (which items bought together)
- Purchase frequency patterns
- Time-based demand analysis

### **Why This Stack?**

| Technology | Purpose | Why Not Alternatives? |
|-----------|---------|---------------------|
| **Iceberg** | Table format | Better than Delta Lake for multi-engine (Spark + DuckDB) |
| **Databricks AWS** | Managed Spark | More production-ready than Databricks Community Edition |
| **MongoDB** | Metadata + Metrics | More flexible than Postgres for nested schemas |
| **DuckDB** | Query engine | Faster than Spark for small analytical queries (<10GB) |
| **dbt-spark** | Transform | SQL-based, version controlled, testable |

---

## 🏛️ Architecture

### **Data Flow**

```
CSV (Kaggle)
    ↓
[1] PySpark Bronze Ingestion (Databricks)
    → s3://bucket/bronze/*.iceberg
    ↓
[2] PySpark Silver Transformation (Databricks)
    → s3://bucket/silver/*.iceberg
    ↓
[3] dbt-spark Gold Layer (Databricks)
    → s3://bucket/gold/*.iceberg
    ↓
┌──────────────┴──────────────┐
│                              │
[4] MongoDB                [5] DuckDB
- Dataset metadata         - Query engine
- Metrics definitions      - Read Iceberg from S3
- Execution tracking       - Materialize metrics
│                              │
└──────────────┬───────────────┘
               │
        [6] FastAPI
    - GET /datasets
    - POST /query
    - GET /metrics
    - POST /metrics/{name}/execute
               │
        [7] Python SDK
               │
            Users
```

### **Technology Stack**

| Layer | Component | Details |
|-------|-----------|---------|
| **Storage** | AWS S3 | Raw data + Iceberg tables (~2GB) |
| **Compute** | Databricks AWS | 14-day trial, m5.large 1-node cluster |
| **Table Format** | Apache Iceberg | ACID, time travel, schema evolution |
| **Transform** | dbt-spark | Dimensional modeling (runs on Databricks) |
| **Metadata** | MongoDB Atlas M0 | Free tier (512MB), ~10KB used |
| **Query** | DuckDB | Embedded, reads Iceberg directly |
| **API** | FastAPI | REST endpoints |
| **SDK** | Python | Wrapper over API |
| **IaC** | Terraform | S3 bucket + IAM provisioning |

### **Cost: $0.00 Total**
- AWS S3: 2GB / 5GB free tier = FREE ✅
- Databricks: 14-day trial = FREE ✅
- MongoDB Atlas: M0 free tier forever = FREE ✅

---

## ✅ Implementation Status

### **Phase 1: Infrastructure (DONE)**

| Component | Status | Files | Notes |
|-----------|--------|-------|-------|
| Terraform config | ✅ Done | `terraform/main.tf` | AWS-only, removed GCP |
| Config centralization | ✅ Done | `config/instacart_config.py` | Single source of config |
| Environment setup | ✅ Done | `.env.example` | All credentials documented |
| Docker compose | ✅ Done | `docker-compose.yml` | MongoDB + API + Mongo Express |

### **Phase 2: Data Pipeline (DONE)**

| Component | Status | Files | Notes |
|-----------|--------|-------|-------|
| Bronze ingestion | ✅ Done | `pyspark/bronze_ingestion.py` | CSV → Iceberg |
| Silver transformation | ✅ Done | `pyspark/silver_transformation.py` | Cleaning + enrichment |
| Data quality checks | ✅ Done | `pyspark/data_quality_checks.py` | Validation rules |
| Gold dbt models | ✅ Done | `dbt_instacart/models/` | Star schema + marts |
| dbt project config | ✅ Done | `dbt_project.yml`, `profiles.yml` | dbt-spark configured |

### **Phase 3: Warehouse Service (DONE)**

| Component | Status | Files | Notes |
|-----------|--------|-------|-------|
| DuckDB engine | ✅ Done | `warehouse/engine.py` | Reads Iceberg from S3 |
| MongoDB metadata | ✅ Done | `warehouse/metadata.py` | Dataset catalog |
| FastAPI endpoints | ✅ Done | `warehouse/main.py` | 3 core endpoints |
| SQL validator | ✅ Done | `warehouse/sql_validator.py` | AST-based (sqlglot) |
| Python SDK | ✅ Done | `warehouse/sdk/client.py` | API wrapper |
| Pydantic models | ✅ Done | `warehouse/models.py` | Request/response types |

### **Phase 4: Metrics Store (NEW - DONE)**

| Component | Status | Files | Notes |
|-----------|--------|-------|-------|
| Metrics engine | ✅ Done | `warehouse/metrics_engine.py` | Execute metrics from MongoDB |
| Metrics API | ✅ Done | `warehouse/main.py` | 8 new endpoints |
| Seed script | ✅ Done | `scripts/seed_metrics.py` | 6 example metrics |
| Test script | ✅ Done | `scripts/test_metrics_api.py` | API validation |

**Total Code Added for Metrics Store: ~500 lines**

---

## 📊 Metrics Store Feature

### **What is Metrics Store?**

**Pattern:** Configuration as Data (not Code)

Business metrics stored as **MongoDB documents** instead of YAML files.

**Decision Rationale:**  
After analyzing top Kaggle notebooks for Instacart dataset, **90% of analytics workload is metrics-heavy** (reorder rates, basket size, top N products). MongoDB as **Metrics Document Store** is perfect fit because:
- ✅ Instacart = Reusable metrics (reorder rate, basket size, department performance)
- ✅ Enables self-service analytics with parameters
- ✅ Tracks execution history and performance
- ✅ **NOT** serving store - data stays in Iceberg (columnar, optimized)
- ✅ Industry pattern (similar to dbt metrics, Cube.dev semantic layer)

See detailed analysis: `MONGODB_USE_CASE_DECISION.md`

### **Key Difference from YAML Approach**

| Aspect | YAML Files (Traditional) | MongoDB (This Project) |
|--------|-------------------------|----------------------|
| Storage | Git repository | MongoDB database |
| Editing | Edit → Commit → Deploy | API call → Instant |
| Parameters | Hardcoded | Runtime substitution |
| History | Git log | Execution tracking |
| Search | grep/find | MongoDB queries |
| Self-service | Need Git access | API access only |

### **Example Metric in MongoDB**

```javascript
{
  "metric_name": "avg_basket_size_by_hour",
  "display_name": "Average Basket Size by Hour",
  "description": "Average products per order by hour of day",
  "owner": "analytics-team",
  "tags": ["basket", "hourly", "behavior"],
  
  // SQL with parameter placeholders
  "sql_template": `
    SELECT 
      order_hour_of_day,
      AVG(products_per_order) as avg_basket_size
    FROM gold.dim_orders
    GROUP BY order_hour_of_day
  `,
  
  "materialization": "table",  // or "view"
  "refresh_schedule": "0 6 * * *",  // Cron
  "enabled": true,
  
  // Auto-tracked metadata
  "last_run_at": "2026-07-12T10:30:00Z",
  "last_run_status": "success",
  "execution_time_ms": 450,
  "execution_count": 15
}
```

### **API Endpoints (8 New)**

```bash
# Create metric (replaces creating YAML file)
POST /metrics/register
{
  "metric_name": "weekly_active_users",
  "sql_template": "SELECT COUNT(DISTINCT user_id) FROM gold.dim_orders",
  ...
}

# List metrics
GET /metrics
GET /metrics?tags=basket,hourly
GET /metrics?owner=product-team

# Get metric details
GET /metrics/{name}

# Execute metric
POST /metrics/{name}/execute
POST /metrics/{name}/execute {"parameters": {"min_orders": 100}}

# Update metric (runtime edit!)
PUT /metrics/{name}
{"description": "Updated description"}

# Delete metric
DELETE /metrics/{name}

# Search metrics
GET /metrics/search/reorder

# Execution history
GET /metrics/{name}/history
```

### **Example Usage**

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# List all metrics
metrics = client.list_metrics()

# Execute simple metric
result = client.execute_metric("avg_basket_size_by_hour")
print(f"Rows: {result['row_count']}, Time: {result['execution_time_ms']}ms")

# Execute with parameters
result = client.execute_metric(
    "top_reordered_products",
    parameters={"min_orders": 150, "limit": 10}
)

# Results materialized in DuckDB
df = client.query("SELECT * FROM metric_top_reordered_products")
print(df)
```

### **6 Seeded Example Metrics**

1. **avg_basket_size_by_hour** - Basket size by hour of day
2. **top_reordered_products** - High reorder rate products (parameterized)
3. **department_demand_summary** - Department aggregates
4. **reorder_vs_new_orders** - Reorder behavior analysis
5. **products_by_add_to_cart_order** - Cart priority (parameterized)
6. **order_dow_distribution** - Day of week patterns

All ready to use after running `python scripts/seed_metrics.py`

---

## 🚀 Execution Plan (Free Tier)

### **Timeline: 7 Days, $0 Cost**

| Day | Task | Duration | Cost |
|-----|------|----------|------|
| **Day 1** | AWS + Databricks + MongoDB setup | 3h | $0 |
| **Day 2** | Download data + Upload to S3 | 2h | $0 |
| **Day 3** | Bronze layer ingestion | 3h | $0 |
| **Day 4** | Silver layer transformation | 3h | $0 |
| **Day 5** | Gold layer (dbt) | 4h | $0 |
| **Day 6** | Warehouse API + Metrics Store | 4h | $0 |
| **Day 7** | Documentation + Screenshots | 3h | $0 |
| **TOTAL** | | **22h** | **$0** |

### **Day 1: Setup Infrastructure**

**Morning (1h):**
```bash
# AWS account + S3
cd terraform
terraform init
terraform apply -auto-approve

# Note bucket name
terraform output s3_bucket_name
```

**Afternoon (2h):**
- Databricks AWS trial signup
  - AWS Marketplace → Subscribe to Databricks
  - Create workspace (10-15 min)
  - Create cluster (m5.large, 1 node)
  - Get token + cluster ID
- MongoDB Atlas M0 setup
  - Signup at mongodb.com/cloud/atlas/register
  - Create M0 cluster (free)
  - Whitelist 0.0.0.0/0
  - Get connection string

**Update `.env`:**
```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=instacart-lakehouse
DATABRICKS_HOST=https://...
DATABRICKS_TOKEN=...
MONGODB_URI=mongodb+srv://...
```

### **Day 2: Data Acquisition**

```bash
# Setup Kaggle API
# Get token from kaggle.com → Settings → API
mkdir ~/.kaggle
mv kaggle.json ~/.kaggle/

# Download dataset (~1.3GB)
python scripts/download_kaggle_dataset.py

# Upload to S3
python scripts/upload_to_s3.py

# Verify
aws s3 ls s3://instacart-lakehouse/raw/instacart/ --recursive
```

### **Day 3-4: Bronze + Silver Layers**

Upload PySpark code to Databricks, run jobs:
```bash
# Day 3: Bronze ingestion
spark-submit pyspark/bronze_ingestion.py

# Day 4: Silver transformation
spark-submit pyspark/silver_transformation.py

# Data quality checks
spark-submit pyspark/data_quality_checks.py
```

**Export notebooks to HTML** (backup before trial ends!)

### **Day 5: Gold Layer (dbt)**

```bash
cd dbt_instacart

# Configure profiles.yml with Databricks credentials

# Run dbt models
dbt run --profiles-dir ~/.dbt --target prod

# Run tests
dbt test --profiles-dir ~/.dbt --target prod

# Generate docs
dbt docs generate --profiles-dir ~/.dbt
dbt docs serve --port 8002

# Screenshot the lineage graph!
```

### **Day 6: Warehouse Service + Metrics Store**

**Morning: Start local services**
```bash
# Start MongoDB
docker-compose up -d mongodb

# Register metadata
python scripts/register_metadata.py
```

**Afternoon: Metrics Store**
```bash
# Seed example metrics
python scripts/seed_metrics.py

# Start API
cd warehouse
uvicorn main:app --reload --port 8000

# Test metrics API
python scripts/test_metrics_api.py
```

**Evening: Python SDK testing**
```python
from warehouse.sdk import WarehouseClient
client = WarehouseClient()
metrics = client.list_metrics()
result = client.execute_metric("avg_basket_size_by_hour")
print(result)
```

### **Day 7: Documentation + Portfolio**

**Screenshots to capture:**
- Databricks cluster dashboard
- S3 bucket structure
- dbt docs lineage graph
- API Swagger docs (http://localhost:8000/docs)
- Metrics execution results
- MongoDB collections

**Git commit:**
```bash
git add .
git commit -m "feat: complete lakehouse with metrics store"
git push
```

**Create presentation:**
- 10-slide deck
- Architecture diagram
- Key metrics results
- Technologies used
- Learnings + challenges

---

## 📁 Project Structure

```
Spark-Iceberg-DuckDB-Lakehouse/
│
├── config/
│   ├── instacart_config.py       # Centralized config (AWS-only)
│   └── __init__.py
│
├── pyspark/
│   ├── bronze_ingestion.py       # CSV → Iceberg Bronze
│   ├── silver_transformation.py  # Bronze → Silver
│   ├── data_quality_checks.py    # Validation
│   ├── market_basket_mining.py   # FPGrowth (optional)
│   └── utils.py
│
├── dbt_instacart/
│   ├── models/
│   │   ├── staging/              # stg_* views
│   │   └── marts/
│   │       ├── dimensions/       # dim_product, dim_orders
│   │       ├── facts/            # fct_order_products
│   │       └── analytics/        # marts
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── warehouse/
│   ├── main.py                   # FastAPI (11 endpoints)
│   ├── engine.py                 # DuckDB engine
│   ├── metadata.py               # MongoDB client
│   ├── metrics_engine.py         # NEW: Metrics execution
│   ├── sql_validator.py          # AST-based validation
│   ├── models.py                 # Pydantic models
│   └── sdk/
│       └── client.py             # Python SDK
│
├── scripts/
│   ├── download_kaggle_dataset.py
│   ├── upload_to_s3.py
│   ├── register_metadata.py
│   ├── seed_metrics.py           # NEW: Seed example metrics
│   └── test_metrics_api.py       # NEW: Test metrics API
│
├── terraform/
│   ├── main.tf                   # S3 + IAM (AWS only)
│   └── variables.tf
│
├── dags/
│   └── instacart_pipeline_dag.py # Airflow orchestration
│
├── docker-compose.yml            # MongoDB + API + Mongo Express
├── Dockerfile.warehouse          # API container
├── .env.example                  # Environment template
├── requirements.txt              # Python dependencies
├── PROJECT_MASTER.md             # THIS FILE
└── README.md                     # Project overview
```

---

## 🎯 Next Steps

### **Immediate (This Week)**

**Option A: Execute Free Tier Plan**
1. Start Day 1 setup (AWS + Databricks + MongoDB)
2. Follow 7-day execution plan
3. Complete pipeline end-to-end
4. Test Metrics Store feature
5. Create portfolio materials

**Option B: Test Locally First**
1. Start Docker services (`docker-compose up -d`)
2. Seed metrics (`python scripts/seed_metrics.py`)
3. Start API (`uvicorn warehouse.main:app`)
4. Test metrics API (`python scripts/test_metrics_api.py`)
5. Verify all features work before deploying

### **After Core Pipeline Complete**

**Week 2 Enhancements (Optional):**
- [ ] Add more example metrics (10+ total)
- [ ] Create Streamlit UI for metrics
- [ ] Add metric execution scheduling
- [ ] Implement metric lineage graph
- [ ] Add alert rules based on metrics
- [ ] Create dashboard builder

### **Portfolio Materials Checklist**

- [ ] GitHub README with screenshots
- [ ] 10-slide presentation deck
- [ ] Architecture diagram (visual)
- [ ] Demo video (5-10 min)
- [ ] LinkedIn post draft
- [ ] Blog post (optional)
- [ ] Resume bullet points

### **Interview Prep**

**Key talking points:**

1. **Medallion Architecture**
   > "Built a lakehouse with Bronze/Silver/Gold layers using Apache Iceberg on S3"

2. **Metrics Store Pattern**
   > "Implemented a metrics store where business logic is stored as data in MongoDB, enabling self-service analytics without code deployment"

3. **Technology Choices**
   > "Used DuckDB for fast analytical queries over Iceberg tables, MongoDB for flexible metadata storage, and dbt for version-controlled transformations"

4. **Cost Optimization**
   > "Architected the entire platform to run on free tiers - AWS S3, Databricks trial, MongoDB Atlas M0 - total cost $0"

5. **Engineering Practices**
   > "AST-based SQL validation using sqlglot, REST API design with FastAPI, Python SDK for easy integration"

---

## 📊 Current Status Summary

| Phase | Status | Completion |
|-------|--------|------------|
| Infrastructure | ✅ Done | 100% |
| Data Pipeline | ✅ Done | 100% |
| Warehouse Service | ✅ Done | 100% |
| Metrics Store | ✅ Done | 100% |
| Documentation | ✅ Done | 100% |
| Deployment | ⏳ Pending | 0% |
| Testing | ⏳ Pending | 0% |

**Ready to execute!** 🚀

---

## 🔗 Key Files Reference

**Getting Started:**
- `README.md` - Project overview
- `.env.example` - Environment setup
- `requirements.txt` - Dependencies

**Pipeline Code:**
- `pyspark/bronze_ingestion.py` - Bronze layer
- `pyspark/silver_transformation.py` - Silver layer
- `dbt_instacart/models/` - Gold layer

**Warehouse Service:**
- `warehouse/main.py` - FastAPI app
- `warehouse/metrics_engine.py` - Metrics execution
- `warehouse/sdk/client.py` - Python SDK

**Setup Scripts:**
- `scripts/seed_metrics.py` - Load example metrics
- `scripts/test_metrics_api.py` - Validate API
- `scripts/register_metadata.py` - Load dataset metadata

**Infrastructure:**
- `terraform/main.tf` - AWS resources
- `docker-compose.yml` - Local services

---

## 💡 Key Insights

### **What Makes This Project Unique?**

1. **Metrics as Data, Not Code**
   - No YAML files
   - Runtime editing via API
   - Self-service for analysts

2. **Free Tier Architecture**
   - Complete lakehouse for $0
   - Production-ready patterns
   - Scales to real workloads

3. **Multi-Engine Approach**
   - Spark for ETL
   - DuckDB for queries
   - MongoDB for metadata
   - Each tool for its strength

4. **Interview-Ready**
   - Modern data stack
   - Clear design decisions
   - Measurable outcomes

---

**Last Updated:** 2026-07-12  
**Status:** Ready for execution  
**Next Action:** Choose Option A or B from Next Steps

