# Warehouse Service

Simple SQL query API for Iceberg Gold layer using DuckDB and MongoDB.

## Architecture

```
FastAPI (3 endpoints)
     │
     ├─► MongoDB (metadata catalog)
     │   - Dataset schemas
     │   - Statistics
     │   - Lineage
     │
     └─► DuckDB (query engine)
         - Reads Iceberg Gold (S3)
         - Embedded, read-only
```

## Setup

### Prerequisites

```bash
pip install fastapi uvicorn duckdb pymongo pydantic pandas
```

### Environment Variables

```bash
# MongoDB
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=instacart_metadata

# S3 / Iceberg
S3_GOLD_PATH=s3://instacart-lakehouse/gold
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
```

## Usage

### Start API Server

```bash
cd warehouse
uvicorn main:app --reload --port 8000
```

### API Endpoints

**1. List Datasets**
```bash
GET /datasets

Response:
[
  {
    "dataset_id": "gold.dim_user",
    "table_name": "dim_user",
    "row_count": 206209,
    "updated_at": "2026-07-10T10:30:00Z"
  }
]
```

**2. Get Dataset Metadata**
```bash
GET /datasets/gold.dim_user

Response:
{
  "dataset_id": "gold.dim_user",
  "schema_name": "gold",
  "table_name": "dim_user",
  "row_count": 206209,
  "location": "s3://instacart-lakehouse/gold/dim_user",
  "schema": [
    {"name": "user_id", "type": "bigint"},
    {"name": "total_orders", "type": "bigint"}
  ],
  "updated_at": "2026-07-10T10:30:00Z"
}
```

**3. Execute SQL Query**
```bash
POST /query
Content-Type: application/json

{
  "sql": "SELECT * FROM gold.dim_user LIMIT 10"
}

Response:
{
  "columns": ["user_id", "total_orders", "avg_basket_size"],
  "rows": [
    [1, 10, 8.5],
    [2, 15, 12.3]
  ],
  "row_count": 10,
  "execution_time_ms": 45.2
}
```

### Python SDK

```python
from warehouse.sdk import WarehouseClient

# Initialize client
client = WarehouseClient("http://localhost:8000")

# List datasets
datasets = client.list_datasets()

# Get metadata
metadata = client.get_dataset("gold.dim_user")

# Query and get DataFrame
df = client.query("SELECT * FROM gold.dim_user LIMIT 100")
print(df.head())

# Close
client.close()
```

## Design Philosophy

**Keep it simple:**
- ❌ No Redis cache
- ❌ No authentication
- ❌ No rate limiting
- ❌ No query logging
- ✅ Focus on core functionality (metadata + queries)

**Total: ~300 lines of code**

## File Structure

```
warehouse/
├── main.py           # FastAPI app (100 lines)
├── engine.py         # DuckDB engine (80 lines)
├── metadata.py       # MongoDB client (60 lines)
├── models.py         # Pydantic models (40 lines)
└── sdk/
    └── client.py     # Python SDK (80 lines)
```

## Testing

```bash
# Start service
uvicorn main:app --reload

# Test endpoints
curl http://localhost:8000/
curl http://localhost:8000/datasets
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM gold.dim_user"}'
```

## MongoDB Pattern

MongoDB stores **metadata only** (not business data):

```javascript
{
  "dataset_id": "gold.dim_user",
  "schema_name": "gold",
  "table_name": "dim_user",
  "row_count": 206209,
  "location": "s3://instacart-lakehouse/gold/dim_user",
  "schema": [
    {"name": "user_id", "type": "bigint"},
    {"name": "total_orders", "type": "bigint"}
  ],
  "table_format": "iceberg",
  "created_at": "2026-07-10T10:00:00Z",
  "updated_at": "2026-07-10T10:30:00Z"
}
```

This pattern mimics:
- Unity Catalog (Databricks)
- Hive Metastore
- AWS Glue Catalog

Business data stays in Iceberg (S3).
