# Instacart Data Lakehouse

**Modern data platform built with PySpark, Apache Iceberg, dbt, MongoDB, and DuckDB**

[![Architecture](https://img.shields.io/badge/Architecture-Lakehouse-blue)](https://www.databricks.com/glossary/data-lakehouse)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🎯 Project Overview

End-to-end data lakehouse pipeline processing 1.3GB of Instacart e-commerce data (33M records) through Bronze → Silver → Gold layers, with metadata catalog and SQL query API.

**Key Features:**
- ✅ **Medallion Architecture** (Bronze/Silver/Gold)
- ✅ **Apache Iceberg** for ACID transactions and time travel
- ✅ **dbt** for dimensional modeling
- ✅ **MongoDB** as metadata catalog (Unity Catalog pattern)
- ✅ **DuckDB** for fast analytical queries
- ✅ **FastAPI** for SQL query service
- ✅ **Terraform** for infrastructure as code

---

## 🏗️ Architecture

```
CSV Data (Kaggle)
      ↓
PySpark (Databricks) → Iceberg Bronze/Silver (S3)
      ↓
dbt-spark (Databricks) → Iceberg Gold (S3)
      ↓
┌──────────────────┴───────────────────┐
│                                       │
MongoDB                           DuckDB
- Dataset metadata                - Query Gold layer
- Schema, stats                   - Embedded
- Lineage                         - Read-only
│                                       │
└──────────────────┬────────────────────┘
                   │
            FastAPI Service
         - GET /datasets
         - GET /datasets/{name}
         - POST /query
                   │
            Python SDK
                   │
               Users
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | AWS S3 | Object storage for Iceberg tables |
| **Compute** | Databricks Community | Free Spark runtime |
| **Table Format** | Apache Iceberg | ACID transactions, time travel |
| **Transform** | dbt-spark | SQL-based dimensional modeling |
| **Metadata** | MongoDB | Dataset catalog (schema, stats, lineage) |
| **Query** | DuckDB | Fast analytical query engine |
| **API** | FastAPI | REST API for SQL queries |
| **Orchestration** | Apache Airflow | Workflow scheduling |
| **IaC** | Terraform | Infrastructure provisioning |

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
- Dimensional model (star schema)
- Optimized for analytics queries
- Materialized via dbt
- 3 dimension tables + 1 fact table

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- AWS account (S3 access)
- Databricks Community account (free)
- MongoDB (local or Atlas free tier)
- Terraform (for infrastructure)

### 1. Clone Repository

```bash
git clone <repo-url>
cd Data-Migration-with-Spark-Airflow-Postgres
```

### 2. Setup Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY
# - DATABRICKS_HOST
# - DATABRICKS_TOKEN
# - MONGODB_URI
```

### 3. Provision Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply

# Note the S3 bucket name from output
```

### 4. Upload Raw Data to S3

```bash
# Download Instacart dataset from Kaggle
python scripts/download_kaggle_dataset.py

# Upload to S3
python scripts/upload_to_s3.py
```

### 5. Run Data Pipeline

```bash
# Bronze ingestion (CSV → Iceberg)
spark-submit pyspark/bronze_ingestion.py

# Silver transformation (cleaning + enrichment)
spark-submit pyspark/silver_transformation.py

# Data quality checks
spark-submit pyspark/data_quality_checks.py

# Gold layer (dbt dimensional model)
cd dbt_instacart
dbt run --profiles-dir ~/.dbt --target prod
dbt test

# Register metadata to MongoDB
python scripts/register_metadata.py
```

### 6. Start Warehouse Service

```bash
# Start FastAPI server
cd warehouse
uvicorn main:app --reload --port 8000

# Test API
curl http://localhost:8000/datasets
```

### 7. Query via Python SDK

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# List available datasets
datasets = client.list_datasets()

# Execute SQL query
df = client.query("""
    SELECT 
        user_id,
        total_orders,
        avg_basket_size
    FROM gold.dim_user
    WHERE user_segment = 'Power'
    LIMIT 10
""")

print(df)
```

---

## 📁 Project Structure

```
.
├── config/                    # Configuration files
│   ├── instacart_config.py   # Centralized config
│   └── __init__.py
├── dags/                      # Airflow DAGs
│   └── instacart_pipeline_dag.py
├── dbt_instacart/            # dbt project
│   ├── models/
│   │   ├── staging/          # Staging views
│   │   ├── marts/
│   │   │   ├── dimensions/   # Dimension tables
│   │   │   └── facts/        # Fact tables
│   ├── profiles.yml          # dbt Spark profile
│   └── dbt_project.yml
├── pyspark/                   # PySpark jobs
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
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
│   ├── engine.py             # DuckDB engine
│   ├── metadata.py           # MongoDB client
│   ├── models.py             # Pydantic models
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

# Databricks
DATABRICKS_HOST=https://community.cloud.databricks.com
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
    total_orders,
    reorder_rate
FROM gold.dim_product
ORDER BY total_orders DESC
LIMIT 10;

-- Orders by day of week
SELECT 
    order_dow,
    COUNT(*) as order_count
FROM gold.fct_order_products
GROUP BY order_dow
ORDER BY order_dow;

-- Power users analysis
SELECT 
    user_segment,
    COUNT(*) as user_count,
    AVG(total_orders) as avg_orders,
    AVG(avg_basket_size) as avg_basket
FROM gold.dim_user
GROUP BY user_segment;
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
Business data stays in Iceberg (S3)

### Why DuckDB?

- **Embedded**: Runs in-process, no separate server
- **Fast**: Columnar engine optimized for analytics
- **Simple**: Reads Iceberg directly from S3
- **Iceberg Support**: Native Iceberg extension

### Why Simplicity?

Intentionally kept simple (no Redis, no auth, no rate limiting) to focus on core functionality:
- Metadata discovery
- SQL query execution
- Data serving

---

## 💰 Cost Estimate

| Service | Usage | Cost |
|---------|-------|------|
| AWS S3 | ~2GB storage | ~$0.05/month |
| Databricks Community | Free tier | $0 |
| MongoDB Atlas | Free tier (512MB) | $0 |
| **Total** | | **~$0-2/month** |

---

## 🛠️ Development

### Run Tests

```bash
pytest tests/ -v --cov
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

- [Setup Guide](SETUP_GUIDE.md) - Detailed setup instructions
- [Architecture](ARCHITECTURE_SIMPLIFIED.md) - Simplified architecture overview
- [Implementation Plan](IMPLEMENTATION_PLAN.md) - Step-by-step implementation
- [Status](STATUS.md) - Current project status

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details

---

## 🤝 Contributing

This is a portfolio project, but feedback and suggestions are welcome!

---

**Built with ❤️ for learning modern data engineering patterns**
