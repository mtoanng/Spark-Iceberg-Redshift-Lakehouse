# 🛒 Instacart Market Basket Lakehouse

**Modern data platform with Metrics Store pattern**

[![Status](https://img.shields.io/badge/status-ready%20to%20deploy-success)]()
[![Cost](https://img.shields.io/badge/cost-$0.00-brightgreen)]()
[![License](https://img.shields.io/badge/license-MIT-blue)]()

---

## 🎯 Project Overview

End-to-end data lakehouse processing **33M+ Instacart orders** through Medallion architecture (Bronze → Silver → Gold) with **Metrics Store** for self-service analytics.

**Unique Feature:** Business metrics stored as data in MongoDB (not YAML files), enabling analysts to create and execute metrics via API without code deployment.

**Why "Market Basket" and not "Sales"?**
The Instacart dataset has **no price/revenue data** — it only captures purchasing behavior (reordered, add_to_cart_order). All modeling revolves around **market basket behavior**: which products are bought together, reorder rates, and demand patterns by time of day. This is a deliberate framing choice, not an oversight.

**Key Features:**
- 🏗️ Medallion Architecture (Bronze/Silver/Gold)
- ⚡ Apache Iceberg for ACID transactions and time travel
- 🔄 dbt for dimensional modeling (star schema)
- 📊 **Metrics Store** - 15 business metrics as MongoDB documents
- 🚀 DuckDB for fast analytical queries
- 🐍 Python SDK for easy integration
- 💰 **$0 cost** - runs entirely on free tiers

**Quick Stats:**
- 33M+ records processed
- 15 predefined business metrics
- <500ms query response time
- 7-day implementation timeline

---

## 🏗️ Architecture

```
CSV Data (Kaggle)
      ↓
PySpark (Databricks) → Iceberg Bronze/Silver (S3)
      ↓
dbt-spark (Databricks) → Iceberg Gold (S3)
      ↓
┌──────────────────┴────────────────────┐
│                                       │
MongoDB                           DuckDB
- Dataset metadata                - Query Gold layer
- Metrics definitions (NEW)       - Execute metrics
- Schema, stats, lineage          - Embedded, read-only
│                                       │
└──────────────────┬────────────────────┘
                   │
            FastAPI Service
         - GET /datasets
         - POST /query
         - GET /metrics (NEW)
         - POST /metrics/{name}/execute (NEW)
                   │
            Python SDK
                   │
               Users
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | AWS S3 | Object storage for Iceberg tables |
| **Compute** | Databricks on AWS | Managed Spark (trial 14-day, not Community Edition) |
| **Table Format** | Apache Iceberg | ACID transactions, time travel (not Delta Lake) |
| **Transform** | dbt-spark | SQL-based dimensional modeling |
| **Metadata** | MongoDB | Dataset catalog + metrics definitions |
| **Query** | DuckDB | Fast analytical query engine (embedded) |
| **SQL Validation** | sqlglot | AST-based read-only enforcement (only SELECT) |
| **Cache** | In-process (Python dict + TTL) | No Redis — known limitation |
| **API** | FastAPI | REST API for SQL queries |
| **Orchestration** | Apache Airflow | Workflow scheduling |
| **IaC** | Terraform | Infrastructure provisioning (S3 + IAM) |

---

## 📊 Data Pipeline

### Layer Details

**Bronze Layer (Raw Landing)**
- Ingests raw CSV files from S3
- Minimal transformation, schema on read
- Iceberg tables with metadata tracking
- ~39M rows across 6 tables

**Silver Layer (Cleaned & Enriched)**
- Data quality checks and deduplication
- Join denormalization for performance
- Partitioned by department_id
- ~34M rows across 4 tables

**Gold Layer (Business-Ready)**
- Dimensional model (star schema): dim_product, dim_orders, fct_order_products
- Analytics marts: mart_product_reorder_rate, mart_department_demand
- Optional: market_basket_rules from FPGrowth (which products are bought together)
- No "fact_sales" — dataset has no revenue data, only market basket behavior

---

## 🚀 Quick Start

**Complete setup in 7 days, $0 cost**

### **Prerequisites:**
- AWS account (free tier)
- Databricks AWS trial (14 days)
- MongoDB Atlas M0 (free forever)
- Kaggle API token

### **Setup:**

```bash
# 1. Clone repository
git clone <repo-url>
cd Spark-Iceberg-DuckDB-Lakehouse

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure credentials
cp .env.example .env
# Edit .env with your credentials

# 4. Follow setup checklist
# See SETUP_CHECKLIST.md for Day 1-7 guide
```

**Full guide:** [SETUP_CHECKLIST.md](SETUP_CHECKLIST.md)

---

## 📊 Metrics Store Feature

Execute business metrics via API without code deployment:

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# Execute metric with parameters
result = client.execute_metric(
    "product_reorder_rate",
    parameters={"min_orders": 100, "limit": 20}
)

print(result['preview'])
# [
#   {"product_name": "Banana", "reorder_percentage": 85.3},
#   {"product_name": "Organic Strawberries", "reorder_percentage": 82.1},
#   ...
# ]
```

**15 predefined metrics across 5 categories:**
- Reorder Behavior (4 metrics)
- Basket Analysis (3 metrics)
- Product Performance (2 metrics)
- Department Performance (2 metrics)
- Temporal Analysis (2 metrics)

---

## 📁 Project Structure

```
.
├── .gitlab-ci.yml            # CI pipeline (warehouse-test, dbt-test, build-image)
├── config/                    # Configuration files
│   ├── instacart_config.py   # Centralized config
│   └── __init__.py
├── dags/                      # Airflow DAGs
│   └── instacart_pipeline_dag.py
├── dbt_instacart/            # dbt project
│   ├── models/
│   │   ├── staging/          # Staging views (stg_orders, stg_products, stg_aisles, stg_departments)
│   │   ├── marts/
│   │   │   ├── dimensions/   # dim_product, dim_orders
│   │   │   ├── facts/        # fct_order_products
│   │   │   └── analytics/    # mart_product_reorder_rate, mart_department_demand
│   ├── profiles.yml          # dbt Spark profile
│   └── dbt_project.yml
├── pyspark/                   # PySpark jobs
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── market_basket_mining.py  # FPGrowth (optional/bonus)
│   └── data_quality_checks.py
├── scripts/                   # Utility scripts
│   ├── download_kaggle_dataset.py
│   ├── upload_to_s3.py
│   └── register_metadata.py
├── terraform/                 # Infrastructure as Code
│   ├── main.tf
│   └── variables.tf
├── warehouse/                 # Warehouse service
│   ├── main.py               # FastAPI app
│   ├── engine.py             # DuckDB engine (query + cache)
│   ├── sql_validator.py      # AST-based SQL validation (sqlglot)
│   ├── metadata.py           # MongoDB client
│   ├── models.py             # Pydantic models
│   ├── cache/
│   │   └── memory_cache.py   # In-process TTL cache
│   ├── tests/
│   │   ├── test_sql_validator.py
│   │   └── test_cache.py
│   └── sdk/
│       └── client.py         # Python SDK
└── requirements.txt
```

---

## 🔧 Configuration

### Environment Variables

```bash
# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET=instacart-lakehouse

# Databricks (AWS workspace — NOT Community Edition)
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=your_token
DATABRICKS_CLUSTER_ID=your_cluster_id

# MongoDB
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=instacart_metadata

# Paths
S3_GOLD_PATH=s3://instacart-lakehouse/gold
```

---

## 📊 Sample Queries

```sql
-- Top 10 most ordered products
SELECT 
    product_name,
    total_order_lines,
    reorder_count,
    reorder_rate
FROM gold.mart_product_reorder_rate
ORDER BY total_order_lines DESC
LIMIT 10;

-- Orders by day of week
SELECT 
    order_dow,
    COUNT(*) as order_count
FROM gold.dim_orders
GROUP BY order_dow
ORDER BY order_dow;

-- Department demand by hour of day
SELECT 
    department,
    order_hour_of_day,
    order_line_count
FROM gold.mart_department_demand
ORDER BY order_line_count DESC
LIMIT 20;

-- Reorder rate by department
SELECT 
    department,
    AVG(reorder_rate) as avg_reorder_rate
FROM gold.mart_department_demand
GROUP BY department
ORDER BY avg_reorder_rate DESC;
```

---

## 🎓 Learning Outcomes

This project demonstrates proficiency in:

✅ **Data Engineering**
- Medallion architecture (Bronze/Silver/Gold)
- Data quality and validation
- Dimensional modeling (star schema)

✅ **Modern Data Stack**
- Apache Iceberg (table format)
- dbt (transformation)
- DuckDB (analytical queries)
- MongoDB (metadata catalog)

✅ **Cloud & Infrastructure**
- AWS S3 (object storage)
- Databricks (managed Spark)
- Terraform (IaC)

✅ **Software Engineering**
- REST API design (FastAPI)
- Python SDK development
- Professional code structure

---

## 📝 Design Decisions

### Why MongoDB for Metadata?

MongoDB serves as a **metadata catalog** (NOT a data store), following the pattern of:
- Unity Catalog (Databricks)
- Hive Metastore (Hadoop)
- AWS Glue Catalog

Stores: dataset schemas, statistics, lineage, quality scores, tags  
Business data stays in Iceberg (S3). One sample document per gold-layer table is seeded
manually via `mongo-init/init-db.js` — no auto-update after dbt build (kept minimal per MVP scope).

### Why DuckDB + sqlglot?

- **DuckDB**: Embedded columnar engine, reads Iceberg directly from S3
- **sqlglot**: AST-based SQL validation — only SELECT/WITH queries pass. This is more
  robust than regex/string matching: it catches multi-statement injection, nested DDL
  inside CTEs, and non-SELECT root statements.

### Why In-Process Cache (not Redis)?

The cache uses a simple Python dict with TTL (300s default). This is intentionally simple:
- No additional service to deploy
- Sufficient for a single-instance MVP

**Known limitation**: The cache is NOT shared across multiple service instances (unlike Redis).
If the service is scaled horizontally, each instance maintains its own cache. This must be
upgraded to Redis or a distributed cache before scaling.

### Why No "Sales"/"Revenue"?

The Instacart dataset has **no price data** — it only captures order behavior (which products
were in which orders, whether they were reordered, and cart sequence). All analytics focus on
**market basket behavior**: co-purchase patterns, reorder rates, and demand by time of day.
There is no `fact_sales` table because there is no revenue to measure.

---

## 💰 Cost Estimate

| Service | Usage | Cost |
|---------|-------|------|
| AWS S3 | ~2GB storage | ~$0.05/month |
| Databricks on AWS | Trial (14-day) | $0 (trial) |
| MongoDB Atlas | Free tier (512MB) | $0 |
| **Total** | | **~$0-2/month** |

**Note**: Databricks on AWS trial expires after 14 days. Plan compute-heavy phases
(Bronze/Silver ingestion, FPGrowth mining) in one continuous run. Export notebooks
before trial expires.

---

## 🛠️ Development

### Run Tests

```bash
# Run warehouse tests (SQL validator + cache)
pytest warehouse/tests/ -v
```

### Code Formatting

```bash
black .
flake8 .
mypy .
```

### Start Local Services

```bash
# MongoDB (Docker)
docker run -d -p 27017:27017 --name mongodb mongo:latest

# Warehouse API
cd warehouse && uvicorn main:app --reload
```

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| **[SETUP_CHECKLIST.md](SETUP_CHECKLIST.md)** | Step-by-step setup guide (Day 0-7) |
| **[PROJECT_MASTER.md](PROJECT_MASTER.md)** | Complete project reference |
| **[ARCHITECTURE_SIMPLIFIED.md](ARCHITECTURE_SIMPLIFIED.md)** | System architecture details |
| **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** | Command cheatsheet |
| **[DOCS_INDEX.md](DOCS_INDEX.md)** | Documentation navigation |
| **[PROJECT_COMPLETE.md](PROJECT_COMPLETE.md)** | Deployment readiness |

---

## 💰 Cost Breakdown

| Service | Usage | Monthly Cost |
|---------|-------|--------------|
| AWS S3 | 2GB / 5GB free tier | **$0** |
| Databricks AWS | 14-day trial | **$0** |
| MongoDB Atlas | M0 free tier (512MB) | **$0** |
| **Total** | | **$0.00** ✅ |

---
