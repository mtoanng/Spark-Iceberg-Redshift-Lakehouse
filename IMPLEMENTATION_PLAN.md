# Implementation Plan - Final Architecture

**Status:** Architecture finalized, ready for implementation

---

## 🎯 Final Simplified Stack

```
Layer 1: ETL (Databricks)
  PySpark → Iceberg Bronze/Silver (S3)

Layer 2: Transform (Databricks)
  dbt-spark → Iceberg Gold (S3)

Layer 3: Serve (Local/Cloud)
  MongoDB (metadata) + DuckDB (queries) + FastAPI (API)
```

**Complexity:** Keep it simple, focus on functionality

---

## ✅ What's Done

### 1. ETL Layer (Bronze/Silver) ✅
- `pyspark/bronze_ingestion.py` - CSV → Iceberg Bronze
- `pyspark/silver_transformation.py` - Clean & enrich
- `pyspark/data_quality_checks.py` - Validation
- **Status:** Code complete, uses S3

### 2. Infrastructure ✅
- `terraform/main.tf` - AWS S3 bucket
- `terraform/variables.tf` - Configuration
- **Status:** Ready (AWS only, no GCP)

### 3. Utilities ✅
- `scripts/setup_kaggle.py` - Kaggle setup
- `scripts/download_kaggle_dataset.py` - Data download
- `scripts/upload_to_s3.py` - S3 upload
- **Status:** Complete

### 4. Configuration ✅
- `config/instacart_config.py` - S3 paths
- `.env.example` - AWS credentials
- **Status:** Ready for S3

---

## ⏳ What Needs To Change

### 1. dbt Layer (Transform) ⏳
**Current:** dbt-bigquery (wrong target)  
**Need:** dbt-spark (Databricks target)

**Changes needed:**
```yaml
# dbt_instacart/profiles.yml
instacart:
  target: databricks
  outputs:
    databricks:
      type: spark
      method: http
      host: <workspace>.cloud.databricks.com
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      http_path: /sql/1.0/warehouses/xxxxx
      schema: gold
```

**Action:** Update dbt profiles to target Databricks

---

### 2. Warehouse Service (NEW) ⏳
**Status:** Not implemented yet

**Need to create:**
```
warehouse/
├── main.py              # FastAPI app
│   - GET /datasets
│   - GET /datasets/{name}
│   - POST /query
│
├── engine.py            # DuckDB engine
│   - connect to Iceberg Gold
│   - execute SQL
│
├── metadata.py          # MongoDB client
│   - register datasets
│   - get metadata
│
├── models.py            # Pydantic models
│
└── sdk/
    └── client.py        # Python SDK
```

**Complexity:** Keep it simple (~300 lines total)

---

## 📋 Implementation Steps

### Step 1: Update dbt (30 min)
```bash
# Install dbt-spark
pip install dbt-spark

# Update profiles
# Update models (change from bigquery to spark syntax if needed)

# Test
dbt run --target databricks
```

### Step 2: Create Warehouse Service (3 hours)

**A. DuckDB Engine (30 min)**
```python
# warehouse/engine.py
import duckdb

class DuckDBEngine:
    def __init__(self, gold_path: str):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")
        self.conn.execute(f"ATTACH '{gold_path}' AS gold (TYPE ICEBERG)")
    
    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()
```

**B. MongoDB Metadata (30 min)**
```python
# warehouse/metadata.py
from pymongo import MongoClient

class MetadataStore:
    def __init__(self, mongo_uri: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.warehouse
        self.datasets = self.db.datasets
    
    def register_dataset(self, metadata: dict):
        self.datasets.insert_one(metadata)
    
    def get_dataset(self, name: str):
        return self.datasets.find_one({"_id": name})
    
    def list_datasets(self):
        return list(self.datasets.find())
```

**C. FastAPI App (1 hour)**
```python
# warehouse/main.py
from fastapi import FastAPI
from engine import DuckDBEngine
from metadata import MetadataStore

app = FastAPI()
engine = DuckDBEngine("s3://bucket/gold")
metadata = MetadataStore("mongodb://localhost:27017")

@app.get("/datasets")
def list_datasets():
    return metadata.list_datasets()

@app.get("/datasets/{name}")
def get_dataset(name: str):
    return metadata.get_dataset(name)

@app.post("/query")
def execute_query(sql: str):
    df = engine.query(sql)
    return df.to_dict(orient="records")
```

**D. Python SDK (1 hour)**
```python
# warehouse/sdk/client.py
import requests
import pandas as pd

class WarehouseClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    def list_datasets(self):
        r = requests.get(f"{self.base_url}/datasets")
        return r.json()
    
    def get_dataset(self, name: str):
        r = requests.get(f"{self.base_url}/datasets/{name}")
        return r.json()
    
    def query(self, sql: str):
        r = requests.post(f"{self.base_url}/query", json={"sql": sql})
        return pd.DataFrame(r.json())
```

### Step 3: Metadata Registration (1 hour)
```python
# scripts/register_metadata.py
# After dbt runs, register Gold tables to MongoDB
from pymongo import MongoClient
import duckdb

mongo = MongoClient("mongodb://localhost:27017")
conn = duckdb.connect()
conn.execute("ATTACH 's3://bucket/gold' AS gold (TYPE ICEBERG)")

# Get tables
tables = conn.execute("SHOW TABLES FROM gold").fetchall()

for table in tables:
    # Get schema
    schema = conn.execute(f"DESCRIBE gold.{table[0]}").fetchdf()
    
    # Get stats
    stats = conn.execute(f"SELECT COUNT(*) FROM gold.{table[0]}").fetchone()
    
    # Register to MongoDB
    mongo.warehouse.datasets.insert_one({
        "_id": f"gold.{table[0]}",
        "schema": schema.to_dict(),
        "row_count": stats[0],
        "location": f"s3://bucket/gold/{table[0]}"
    })
```

### Step 4: Test End-to-End (30 min)
```python
# Test the full stack
from warehouse_client import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# Browse datasets
datasets = client.list_datasets()
print(datasets)

# Get metadata
meta = client.get_dataset("gold.fact_order_products")
print(meta)

# Query data
df = client.query("""
    SELECT product_name, SUM(quantity) as total
    FROM gold.fact_order_products f
    JOIN gold.dim_product p ON f.product_id = p.product_id
    GROUP BY product_name
    LIMIT 10
""")
print(df)
```

---

## 🎯 Current Priority

1. ✅ **Keep existing ETL layer** (Bronze, Silver on Databricks)
2. ⏳ **Update dbt** to use dbt-spark (Databricks target)
3. ⏳ **Build warehouse service** (300 lines, simple)
4. ⏳ **Test end-to-end**

---

## 📊 Estimated Time

| Task | Time | Status |
|------|------|--------|
| dbt-spark update | 30 min | ⏳ |
| DuckDB engine | 30 min | ⏳ |
| MongoDB metadata | 30 min | ⏳ |
| FastAPI app | 1 hour | ⏳ |
| Python SDK | 1 hour | ⏳ |
| Metadata registration | 1 hour | ⏳ |
| Testing | 30 min | ⏳ |
| **Total** | **5-6 hours** | |

---

## 🚫 What NOT To Build

- ❌ Redis cache
- ❌ Authentication
- ❌ Rate limiting
- ❌ Query logging
- ❌ Complex abstractions
- ❌ Multiple engine support

**Keep it simple!**

---

## ✅ Success Criteria

When complete:
1. PySpark creates Gold layer via dbt-spark on Databricks
2. MongoDB stores Gold table metadata
3. DuckDB can query Gold tables from S3
4. FastAPI serves 3 endpoints
5. Python SDK works end-to-end
6. Total code < 500 lines for warehouse service

---

**Ready for implementation!** 🚀
