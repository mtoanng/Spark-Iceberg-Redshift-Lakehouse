# Simplified Architecture - Focus on Core Functionality

**Keep it simple: ETL + Transform + Serve**

---

## 🎯 Final Simplified Architecture

```
CSV/JSON (Kaggle)
    ↓
PySpark (Databricks) → Iceberg Bronze/Silver (S3)
    ↓
dbt-spark → Iceberg Gold (S3)
                     ↓
  ┌──────────────────┴──────────────────┐
  │                                     │
MongoDB                             DuckDB
- Dataset metadata              - Query Gold layer
- Schema, stats                 - Embedded
- Lineage                       - Read-only
  │                                     │
  └────────────────┬────────────────────┘
                   │
            FastAPI (Simple)
         - GET /datasets
         - GET /datasets/{name}
         - POST /query
                   │
            Python SDK
                   │
               Users
```

---

## 🔧 Components (Minimal)

### 1. MongoDB - Metadata ONLY
```python
# Simple schema
{
  "_id": "gold.fact_orders",
  "schema_name": "gold",
  "table_name": "fact_orders",
  "columns": [...],
  "row_count": 1000000,
  "location": "s3://bucket/gold/fact_orders"
}
```

### 2. DuckDB - Query ONLY
```python
# Simple engine
class DuckDBEngine:
    def __init__(self, iceberg_path):
        self.conn = duckdb.connect()
        self.conn.execute(f"ATTACH '{iceberg_path}' AS gold (TYPE ICEBERG)")
    
    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()
```

### 3. FastAPI - 3 Endpoints ONLY
```python
@app.get("/datasets")
def list_datasets():
    # Query MongoDB
    return mongo.datasets.find({}, {"_id": 1, "row_count": 1})

@app.get("/datasets/{name}")
def get_dataset(name: str):
    # Query MongoDB
    return mongo.datasets.find_one({"_id": name})

@app.post("/query")
def execute_query(sql: str):
    # Query DuckDB
    return duckdb_engine.query(sql).to_dict()
```

---

## ❌ Remove These (Over-engineering)

- ❌ Redis cache
- ❌ Authentication/API keys
- ❌ Rate limiting
- ❌ Query history logging
- ❌ Connection pooling
- ❌ Arrow IPC optimization
- ❌ Prepared statements
- ❌ SQL validator (just try/catch)
- ❌ Complex service layers

---

## ✅ Keep These (Core)

- ✅ MongoDB for metadata
- ✅ DuckDB for queries
- ✅ FastAPI with 3 endpoints
- ✅ Simple Python SDK
- ✅ Basic error handling

---

## 📦 Simplified Project Structure

```
warehouse/
├── main.py                 # FastAPI app (100 lines)
├── engine.py              # DuckDB engine (50 lines)
├── metadata.py            # MongoDB client (50 lines)
├── models.py              # Pydantic models (30 lines)
└── sdk/
    └── client.py          # Python SDK (80 lines)
```

**Total: ~300 lines of code for entire warehouse service!**

---

## 🚀 Implementation Focus

### Core Functionality Only:
1. **Metadata API** - Browse datasets from MongoDB
2. **Query API** - Execute SQL on DuckDB
3. **SDK** - Simple client library

### Skip These:
- No caching (DuckDB is fast enough)
- No auth (local/internal use)
- No logging (keep it simple)
- No complex abstractions

---

## 💡 Interview Value Still High

**Simple doesn't mean less impressive:**

> "I built a warehouse service with FastAPI that serves Iceberg Gold tables through DuckDB and provides metadata discovery via MongoDB. The key learning was separation of concerns: MongoDB for metadata catalog (like Unity Catalog), DuckDB for fast queries, and a clean API layer. I kept it simple and functional rather than adding unnecessary complexity."

**Shows you understand:**
- ✅ When to add complexity vs keep it simple
- ✅ Core architecture patterns (metadata vs data)
- ✅ Production concepts without over-engineering
- ✅ Focus on functionality, not features

---

## 📋 Implementation Checklist

**Phase 1: Core (2-3 hours)**
- [ ] FastAPI app with 3 endpoints
- [ ] DuckDB engine (read Iceberg)
- [ ] MongoDB metadata store
- [ ] Basic Python SDK

**Phase 2: Data (1-2 hours)**
- [ ] Update dbt to dbt-spark
- [ ] Metadata registration script
- [ ] Test queries

**Phase 3: Polish (1 hour)**
- [ ] README with examples
- [ ] Simple Streamlit UI (optional)
- [ ] Documentation

**Total: 4-6 hours to complete**

---

**Keep it simple, keep it functional!** ✅
