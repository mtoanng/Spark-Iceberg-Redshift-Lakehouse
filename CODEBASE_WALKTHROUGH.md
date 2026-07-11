# Codebase Walkthrough — Instacart Open Lakehouse

> **Purpose**: Map the entire stack end-to-end. After reading this, you should understand every file, every data flow, and how each component connects.

---

## 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INSTACART OPEN LAKEHOUSE                          │
│                                                                             │
│  CSV Files ──► S3 (raw) ──► PySpark ──► Iceberg ──► dbt ──► Iceberg         │
│   (Kaggle)    (Bronze)     (Silver)     (S3)       (Gold)     (S3)          │
│                                                                 │           │
│                                              ┌──────────────────┘           │
│                                              ▼                              │
│                              ┌──────────────────────────────┐               │
│                              │     QUERY GATEWAY (API)      │               │
│                              │  FastAPI + DuckDB + MongoDB  │               │
│                              │                              │               │
│                              │  /query  → SQL on DuckDB     │               │
│                              │  /health → DuckDB+Mongo ping │               │
│                              │  /datasets → catalog lookup  │               │
│                              │  /contracts → data contracts │               │
│                              │  /history → query log        │               │
│                              └──────────┬───────────────────┘               │
│                                         │                                   │
│                              ┌──────────┴────────────────────┐              │
│                              │     MONGODB (Control Plane)   │              │
│                              │                               │              │
│                              │  datasets    — table metadata │              │
│                              │  data_contracts — expectations│              │
│                              │  query_history — audit log    │              │
│                              └───────────────────────────────┘              │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  AIRFLOW (Orchestrator)                                             │    │
│  │  check_s3 → upload → bronze → silver → [fp-growth] → dbt → mongo    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  TERRAFORM (Infrastructure)                                         │    │
│  │  S3 bucket + IAM user (spark) + lifecycle policies                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

**The core idea**: Raw CSV → 3-layer medallion architecture (Bronze/Silver/Gold) stored as Iceberg on S3. MongoDB holds *metadata about the data* (not the data itself). The Warehouse API (Query Gateway) lets you SQL-query the Gold layer via DuckDB, with caching, SQL injection protection, query history, and data contracts.

---

## 2. Directory Structure Map

```
Spark-Iceberg-DuckDB-Lakehouse/
│
├── config/
│   └── instacart_config.py        ← Central config: S3 paths, Spark settings, MongoDB URI
│
├── pyspark/                       ← Spark jobs (the ETL engine)
│   ├── bronze_ingestion.py        ← CSV → Iceberg Bronze
│   ├── silver_transformation.py   ← Bronze → Iceberg Silver (clean, join, deduplicate)
│   ├── market_basket_mining.py    ← OPTIONAL: FPGrowth → gold.market_basket_rules
│   ├── data_quality_checks.py     ← Spark-side DQ validation
│   └── utils.py                   ← Shared Spark helpers (schema validation, null checks)
│
├── dbt_instacart/                 ← dbt project (Silver → Gold dimensional model)
│   ├── dbt_project.yml            ← dbt config: materializations, schemas
│   ├── profiles.yml               ← Connection: Spark Thrift (localhost:1515)
│   ├── packages.yml               ← dbt-utils dependency
│   └── models/
│       ├── sources.yml            ← Source definitions + column tests
│       ├── staging/               ← Views reading from Iceberg Silver
│       │   ├── stg_orders.sql
│       │   ├── stg_products.sql
│       │   ├── stg_order_products.sql
│       │   ├── stg_aisles.sql
│       │   └── stg_departments.sql
│       └── marts/                 ← Gold layer dimensional model
│           ├── dimensions/
│           │   ├── dim_product.sql    ← Product dimension (SCD Type 1)
│           │   └── dim_orders.sql     ← Order dimension (SCD Type 1)
│           ├── facts/
│           │   └── fct_order_products.sql  ← Core fact table
│           └── analytics/
│               ├── mart_product_reorder_rate.sql
│               └── mart_department_demand.sql
│
├── warehouse/                     ← Query Gateway API (FastAPI)
│   ├── main.py                    ← FastAPI app: all endpoints
│   ├── engine.py                  ← DuckDB engine (Iceberg reader + cache)
│   ├── metadata.py                ← MongoDB client (catalog + contracts + history)
│   ├── models.py                  ← Pydantic models (request/response schemas)
│   ├── sql_validator.py           ← AST-based SQL injection protection (sqlglot)
│   ├── cache/
│   │   └── memory_cache.py        ← In-process TTL cache (replaces Redis)
│   ├── sdk/
│   │   └── client.py              ← Python SDK for the API
│   └── tests/
│       ├── test_sql_validator.py  ← 15 tests for SQL validation
│       └── test_cache.py          ← 8 tests for cache behavior
│
├── dags/
│   └── instacart_pipeline_dag.py  ← Airflow DAG (full pipeline orchestration)
│
├── scripts/                       ← Utility scripts
│   ├── download_kaggle_dataset.py ← Download Instacart dataset
│   ├── upload_to_s3.py            ← Upload raw CSVs to S3
│   ├── register_metadata.py       ← Register Gold tables → MongoDB catalog
│   ├── validate_iceberg_tables.py ← Validate Iceberg table structure
│   ├── explore_data_local.py      ← Local data exploration (no cloud needed)
│   └── setup_kaggle.py            ← Kaggle API setup helper
│
├── mongo-init/
│   └── init-db.js                 ← MongoDB bootstrap: collections, indexes, seed data
│
├── terraform/
│   ├── main.tf                    ← AWS infra: S3 + IAM (spark user)
│   ├── variables.tf               ← Terraform variables
│   └── terraform.tfvars.example   ← Example variable values
│
├── docker-compose.yml             ← MongoDB + Warehouse API + Mongo Express
├── Dockerfile.warehouse           ← Warehouse API container image
├── .env.example                   ← Environment variable template
├── Makefile                       ← Quick commands (docker-up, dbt-run, tf-apply, etc.)
└── .gitlab-ci.yml                 ← CI: warehouse tests + dbt tests
```

---

## 3. End-to-End Data Flow

Here's how data flows from raw CSV to the Query Gateway, step by step:

```
Step 1: DOWNLOAD
  Kaggle API → data/raw/instacart/*.csv
  Script: scripts/download_kaggle_dataset.py

Step 2: UPLOAD
  data/raw/instacart/*.csv → s3://bucket/raw/instacart/
  Script: scripts/upload_to_s3.py

Step 3: BRONZE INGESTION (PySpark)
  s3://bucket/raw/instacart/*.csv
    → spark-submit pyspark/bronze_ingestion.py
    → Reads CSV with schema inference
    → Validates row counts
    → Writes to Iceberg Bronze tables on S3:
        iceberg.bronze.orders
        iceberg.bronze.order_products_prior
        iceberg.bronze.products
        iceberg.bronze.aisles
        iceberg.bronze.departments

Step 4: SILVER TRANSFORMATION (PySpark)
  Iceberg Bronze
    → spark-submit pyspark/silver_transformation.py
    → Unions order_products_prior + order_products_train
    → Joins with products, aisles, departments (creates hierarchy)
    → Deduplicates by (order_id, product_id)
    → Writes to Iceberg Silver tables:
        iceberg.silver.orders_enriched
        iceberg.silver.order_products_enriched
        iceberg.silver.products_hierarchy
        iceberg.silver.user_order_summary

Step 5: MARKET BASKET MINING (OPTIONAL — PySpark + MLlib)
  Iceberg Silver
    → spark-submit pyspark/market_basket_mining.py
    → FPGrowth algorithm (minSupport=0.001, minConfidence=0.05)
    → Writes association rules to:
        iceberg.gold.market_basket_rules
  NOTE: This is a bonus differentiator, NOT a blocking dependency.

Step 6: DBT RUN (Silver → Gold dimensional model)
  Iceberg Silver (via Spark Thrift on localhost:1515)
    → dbt run --select staging  (creates views)
    → dbt run --select marts    (creates Iceberg tables)
    → Writes to Iceberg Gold:
        gold.dim_product        (dimension: products + aisle + department)
        gold.dim_orders         (dimension: order attributes)
        gold.fct_order_products (fact: order-product associations)
        gold.mart_product_reorder_rate  (analytics: reorder rate per product)
        gold.mart_department_demand     (analytics: demand by day/hour)

Step 7: DBT TEST
  → dbt test (runs all column tests defined in sources.yml + schema.yml)
  → Tests: unique, not_null, accepted_values, relationships

Step 8: REGISTER METADATA
  Iceberg Gold tables
    → scripts/register_metadata.py
    → Spark reads table stats (row count, schema, location)
    → Writes metadata documents to MongoDB:
        instacart_metadata.datasets collection

Step 9: QUERY GATEWAY (API serves the data)
  User → POST /query {"sql": "SELECT * FROM gold.dim_product LIMIT 10"}
    → sqlglot validates (AST parse, rejects non-SELECT)
    → DuckDB executes (reads Iceberg Gold from S3)
    → Result cached in-process (TTL 5 min)
    → Query recorded in MongoDB (query_history)
    → Response returned with columns, rows, timing, cache_hit flag
```

---

## 4. Component Deep-Dive

### 4.1 Config — `config/instacart_config.py`

The single source of truth for all configuration. Every other module imports from here.

**Key sections:**
| Section | What it configures |
|---------|-------------------|
| Project Paths | `PROJECT_ROOT`, `DATA_DIR`, `DATA_RAW_DIR` |
| AWS S3 | Bucket name, region, S3 URIs for each medallion layer |
| Instacart Files | Expected CSV filenames and row counts (for validation) |
| Spark Configs | AQE, shuffle partitions, Iceberg catalog, S3A filesystem, compression |
| Iceberg | Warehouse path, table names (bronze/silver), table properties |
| DuckDB | Database path, memory limit, thread count |
| MongoDB | URI, database name, collection names |
| Airflow | DAG ID, schedule interval |
| Data Quality | Required columns, unique keys, not-null constraints per table |

**Important design choice**: S3 paths use `s3a://` for Spark (Hadoop connector) but `s3://` for DuckDB (different driver).

### 4.2 PySpark Jobs — `pyspark/`

All Spark jobs follow the same pattern:
1. Import config from `instacart_config.py`
2. Create a `SparkSession` with Iceberg + S3 configs applied
3. Read from Iceberg tables (or S3 CSVs for bronze)
4. Transform
5. Write to Iceberg tables
6. Validate output

**`bronze_ingestion.py`** (357 lines)
- Reads 6 CSV files from S3 raw path
- Defines explicit schemas (not inferred) for type safety
- Validates row counts against `EXPECTED_ROW_COUNTS`
- Writes each as an Iceberg table to `iceberg.bronze.*`
- Uses `format-version: 2` (Iceberg v2 for row-level deletes)

**`silver_transformation.py`** (392 lines)
- Creates 4 Silver tables:
  - `orders_enriched` — orders with user-level metrics (first_order, last_order, recency)
  - `order_products_enriched` — union of prior+train, joined with product hierarchy
  - `products_hierarchy` — flattened department → aisle → product
  - `user_order_summary` — per-user aggregates (total_orders, avg_basket_size, reorder_rate)
- Deduplication using `row_number()` window function
- Referential integrity validation

**`market_basket_mining.py`** (192 lines)
- Uses Spark MLlib `FPGrowth` algorithm
- `minSupport=0.001` (rule must appear in ~3,400+ orders)
- `minConfidence=0.05` (5% — intentionally low because basket data is sparse)
- Writes association rules to `iceberg.gold.market_basket_rules`
- This is the portfolio differentiator — shows ML on top of the lakehouse

**`data_quality_checks.py`** (362 lines)
- Spark-side validation: null checks, unique checks, referential integrity
- Reads quality rules from `config/instacart_config.py` → `DATA_QUALITY_RULES`
- Reports pass/fail per table per rule

**`utils.py`** (398 lines)
- `validate_schema()` — ensures DataFrame has expected columns
- `check_null_counts()` — counts nulls per column
- `check_duplicates()` — finds duplicate rows
- `check_referential_integrity()` — validates FK relationships
- `write_iceberg()` — standardized Iceberg write function
- `create_spark_session()` — reusable session builder

### 4.3 dbt Project — `dbt_instacart/`

Transforms Silver → Gold using the dbt-spark adapter via Thrift connection.

**Connection**: `profiles.yml` → `method: thrift`, `host: localhost`, `port: 1515`
This means dbt sends SQL to a running Spark session (Spark Thrift Server).

**Model layers:**

```
staging/ (views)          marts/ (tables + views)
┌─────────────────┐       ┌─────────────────────────────┐
│ stg_orders       │──┐    │ dimensions/                  │
│ stg_products     │──┤    │   dim_product   (table)      │
│ stg_order_products│─┤───►│   dim_orders    (table)      │
│ stg_aisles       │  │    │ facts/                       │
│ stg_departments  │──┘    │   fct_order_products (table) │
└─────────────────┘       │ analytics/                   │
                           │   mart_product_reorder_rate  │
  Source: Iceberg Silver   │   mart_department_demand     │
  (via Spark Thrift)       └─────────────────────────────┘
                            Target: Iceberg Gold
```

**`dbt_project.yml` key decisions:**
- Staging = `view` (lightweight, no storage cost)
- Dimensions/Facts = `table` (materialized as Iceberg tables for query performance)
- Analytics = `view` (derived from facts, no need to store)
- All output goes to `schema: gold` in Iceberg format

**Testing** (`sources.yml` + `schema.yml`):
- Column-level tests: `unique`, `not_null`, `accepted_values`, `relationships`
- Source freshness checks: warn after 24h, error after 48h
- Tests run via `dbt test` in the Airflow DAG

### 4.4 Warehouse API (Query Gateway) — `warehouse/`

This is the "product" — the API that makes the lakehouse queryable. It's not just a DuckDB wrapper; it implements the **Query Gateway pattern** (like a mini Dremio/Athena).

**`main.py`** — FastAPI application with 7 endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Basic health check (service name + version) |
| `/health` | GET | Deep health check (pings DuckDB + MongoDB) |
| `/datasets` | GET | List all datasets from MongoDB catalog |
| `/datasets/{id}` | GET | Get full metadata for one dataset |
| `/contracts/{table}` | GET | Get data contract (expectations) for a table |
| `/query` | POST | Execute SQL on DuckDB (read-only) |
| `/history` | GET | Recent query execution log |

**Request flow for `POST /query`:**
```
1. Receive SQL string
2. sqlglot parses it into an AST
3. Walk the AST — reject if any forbidden node (INSERT, UPDATE, DROP, etc.)
4. Verify root node is SELECT/UNION/CTE
5. Check in-process cache (MD5 hash of SQL → TTL lookup)
6. If cache hit → return cached result (cache_hit: true)
7. If cache miss → DuckDB executes against Iceberg on S3
8. Store result in cache
9. Record query in MongoDB (query_history collection)
10. Return: columns, rows, row_count, execution_time_ms, cache_hit
```

**`engine.py`** — DuckDB engine:
- In-memory DuckDB connection
- Loads Iceberg extension (`INSTALL iceberg; LOAD iceberg`)
- Configures S3 access via AWS credentials from env
- `query()` method: cache lookup → execute → cache store → return
- Cache key = MD5 hash of SQL string
- Cache TTL = 300 seconds (5 minutes)

**`sql_validator.py`** — AST-based SQL validation:
- Uses `sqlglot` library to parse SQL into an Abstract Syntax Tree
- Walks every node in the tree looking for forbidden types:
  `Insert, Update, Delete, Drop, Create, Alter, TruncateTable, Command, Merge, Vacuum`
- Rejects multi-statement input (only 1 statement allowed)
- Verifies root node is a read-only type (`Select, Subquery, Union, Intersect, Except`)
- This catches tricks that naive string matching would miss (e.g., `WITH x AS (DROP TABLE ...) SELECT * FROM x`)

**`cache/memory_cache.py`** — In-process TTL cache:
- Module-level dict: `{cache_key: (timestamp, value)}`
- `@cached(ttl_seconds=300)` decorator pattern
- `clear_cache()` and `cache_size()` utilities
- Known trade-off: not shared across instances (would need Redis for that)

**`metadata.py`** — MongoDB client with 3 responsibilities:
1. **Dataset catalog**: `list_datasets()`, `get_dataset()`, `register_dataset()`
2. **Query history**: `record_query()`, `get_query_history()`
3. **Data contracts**: `get_contract()`, `register_contract()`

**`models.py`** — Pydantic schemas:
- `QueryRequest` — input: SQL string (1-10000 chars)
- `QueryResponse` — output: columns, rows, row_count, execution_time_ms, cache_hit
- `DatasetMetadata` — table metadata (id, schema, location, row_count)
- `DataContract` — table expectations (not_null, unique, etc.)
- `ErrorResponse` — error + detail

### 4.5 MongoDB (Control Plane) — `mongo-init/init-db.js`

MongoDB is the **metadata catalog** — like a mini Unity Catalog or Hive Metastore.
It does NOT store business data. Business data lives in Iceberg on S3.

**Collections:**

| Collection | Purpose | Populated by |
|-----------|---------|-------------|
| `datasets` | Table metadata (schema, row count, location, columns, quality score) | `scripts/register_metadata.py` |
| `data_contracts` | Expectations per table (not_null, unique) | Seeded by `init-db.js` |
| `query_history` | Audit log of every query through the API | `warehouse/metadata.py` → `record_query()` |
| `schemas` | (Reserved) Schema definitions | — |
| `statistics` | (Reserved) Dataset statistics | — |
| `quality_metrics` | (Reserved) Quality scores | — |
| `lineage` | (Reserved) Data lineage tracking | — |

**Seed data** (from `init-db.js`):
- 5 dataset documents (one per Gold table): fct_order_products, dim_product, dim_orders, mart_product_reorder_rate, mart_department_demand
- 3 data contracts: fct_order_products (not_null: order_id+product_id, unique: order_id+product_id), dim_product (not_null+unique: product_id), dim_orders (not_null+unique: order_id)

### 4.6 Airflow DAG — `dags/instacart_pipeline_dag.py`

Orchestrates the entire pipeline. Runs weekly (Monday 2 AM).

```
check_raw_data → upload_to_s3 → bronze_ingestion → validate_bronze
                                                     │
                              ┌──────────────────────┘
                              ▼
                        silver_transformation → validate_silver
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
            market_basket_mining   data_quality_checks
                    │                    │
                    └────────┬───────────┘
                             ▼
                     dbt_deps → dbt_run_staging → dbt_run_marts → dbt_test
                             │
                             ▼
                    register_metadata → generate_docs → success_notification
```

**Key design decisions:**
- Market basket mining is **non-blocking** — if it fails, the pipeline continues
- Data quality checks run in parallel with market basket mining
- dbt runs after both DQ and FPGrowth complete
- All Spark jobs use `spark-submit --master local[*]` (local mode for dev)

### 4.7 Terraform — `terraform/main.tf`

Provisions AWS infrastructure:

| Resource | Purpose |
|----------|---------|
| `aws_s3_bucket.lakehouse` | Iceberg storage (Bronze + Silver + Gold) |
| `aws_s3_bucket_versioning` | Data protection (keep old versions) |
| `aws_s3_bucket_lifecycle_configuration` | Cost optimization (transition to IA after 90 days) |
| `aws_s3_bucket_public_access_block` | Security (block all public access) |
| `aws_iam_user.spark` | Service account for Spark (replaces old `databricks` user) |
| `aws_iam_access_key.spark` | Access keys for the Spark user |
| `aws_iam_user_policy.spark_s3` | S3 access policy (ListBucket + GetObject + PutObject + DeleteObject) |

**Outputs**: bucket name, ARN, access key ID, secret access key (sensitive)

### 4.8 Docker Compose — `docker-compose.yml`

Three services for local development:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `mongodb` | mongo:7.0 | 27017 | Metadata catalog |
| `warehouse-api` | custom (Dockerfile.warehouse) | 8000 | Query Gateway API |
| `mongo-express` | mongo-express:1.0.2 | 8081 | MongoDB web UI (debugging) |

The warehouse API container:
- Depends on MongoDB being healthy
- Mounts `./warehouse` and `./config` for hot-reload
- Mounts `~/.aws` for AWS credentials
- Receives env vars: MONGODB_URI, AWS keys, S3_GOLD_PATH

### 4.9 CI/CD — `.gitlab-ci.yml`

Three stages:

| Stage | What it does | When it runs |
|-------|-------------|-------------|
| `warehouse-test` | Runs pytest on `warehouse/tests/` | Every MR + main push |
| `dbt-test` | Runs `dbt test` against Spark Thrift | Only when `SPARK_HOST` + `SPARK_PORT` CI vars are set |
| `build-warehouse-image` | Builds Docker image | Manual trigger on main |

---

## 5. Query Gateway — How It Works

The Query Gateway is the key architectural pattern that makes this project more than "a DuckDB wrapper."

**Concept**: The developer thinks they're querying PostgreSQL, but under the hood:
- SQL is validated via AST parsing (sqlglot)
- DuckDB reads Iceberg tables directly from S3
- Results are cached in-process with TTL
- Every query is recorded in MongoDB for auditing
- Data contracts define expectations for the tables being queried

```
          ┌─────────────────────────────────────────────┐
          │              QUERY GATEWAY                   │
          │                                             │
Client ──►│  POST /query {"sql": "SELECT ..."}          │
          │       │                                     │
          │       ▼                                     │
          │  ┌──────────────┐                           │
          │  │ sqlglot AST  │──► REJECT if not SELECT   │
          │  │  Validator   │                           │
          │  └──────┬───────┘                           │
          │         │ PASS                              │
          │         ▼                                   │
          │  ┌──────────────┐                           │
          │  │ In-Process   │──► HIT? Return cached     │
          │  │ TTL Cache    │                           │
          │  └──────┬───────┘                           │
          │         │ MISS                              │
          │         ▼                                   │
          │  ┌──────────────┐                           │
          │  │   DuckDB     │──► Reads Iceberg from S3  │
          │  │   Engine     │                           │
          │  └──────┬───────┘                           │
          │         │                                   │
          │    ┌────┴────┐                              │
          │    ▼         ▼                              │
          │  Cache     MongoDB                          │
          │  Store     (query_history)                  │
          │                                             │
          │  ◄── Return: columns, rows, timing, cache_hit│
          └─────────────────────────────────────────────┘
```

**Endpoints summary:**

| Endpoint | What it proves |
|----------|---------------|
| `GET /health` | "I can verify connectivity to both my query engine and metadata catalog" |
| `POST /query` | "I can execute safe SQL against Iceberg data on S3, with caching and audit" |
| `GET /datasets` | "I can browse what tables exist (from the metadata catalog)" |
| `GET /contracts/{table}` | "I can see what data quality expectations exist for a table" |
| `GET /history` | "I can see what queries have been run (audit trail)" |

---

## 6. Data Contracts — How They Work

Data contracts are stored in MongoDB and define expectations for each table.

```json
{
    "table": "gold.fct_order_products",
    "expectations": {
        "not_null": ["order_id", "product_id"],
        "unique": ["order_id", "product_id"]
    }
}
```

**Current contracts:**

| Table | not_null | unique |
|-------|----------|--------|
| `gold.fct_order_products` | order_id, product_id | order_id, product_id |
| `gold.dim_product` | product_id | product_id |
| `gold.dim_orders` | order_id | order_id |

**How they're used:**
1. Seeded at startup by `mongo-init/init-db.js`
2. Queryable via `GET /contracts/{table}`
3. Registerable via `metadata_store.register_contract()`
4. Future: Spark pipeline could read these at runtime to validate data during ingestion

---

## 7. MongoDB as Metadata Catalog

MongoDB plays the role of a **control plane** — like Unity Catalog or Hive Metastore, but simpler.

```
┌──────────────────────────────────────────────────────────┐
│                    MONGODB                                │
│                  (Control Plane)                          │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ datasets collection                                 │ │
│  │                                                     │ │
│  │  { dataset_id: "gold.fct_order_products",          │ │
│  │    schema_name: "gold",                             │ │
│  │    table_name: "fct_order_products",                │ │
│  │    row_count: 33819106,                             │ │
│  │    location: "s3://instacart-lakehouse/gold/...",   │ │
│  │    table_format: "iceberg",                         │ │
│  │    columns: [{name, type, description}, ...],       │ │
│  │    quality_score: 0.97,                             │ │
│  │    tags: ["market-basket", "core-fact"] }           │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ data_contracts collection                           │ │
│  │                                                     │ │
│  │  { table: "gold.fct_order_products",               │ │
│  │    expectations: { not_null: [...], unique: [...] }}│ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │ query_history collection                            │ │
│  │                                                     │ │
│  │  { sql: "SELECT ...",                               │ │
│  │    duration_ms: 42.5,                               │ │
│  │    row_count: 100,                                  │ │
│  │    cache_hit: false,                                │ │
│  │    executed_at: ISODate("...") }                    │ │
│  └────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

**Key principle**: MongoDB stores metadata *about* the data, not the data itself. Business data lives in Iceberg on S3. If MongoDB goes down, the data is still safe in S3 — you just lose the catalog.

---

## 8. Infrastructure (Terraform + Docker)

### AWS Resources (Terraform)

```
┌─────────────────────────────────────────────────────┐
│                    AWS                               │
│                                                     │
│  ┌───────────────────────────────────────────────┐ │
│  │  S3 Bucket: instacart-lakehouse-xxxx          │ │
│  │                                               │ │
│  │  raw/instacart/   ← CSV files                │ │
│  │  bronze/          ← Iceberg Bronze tables     │ │
│  │  silver/          ← Iceberg Silver tables     │ │
│  │  gold/            ← Iceberg Gold tables       │ │
│  │                                               │ │
│  │  Versioning: Enabled                          │ │
│  │  Lifecycle: → IA after 90d, expire after 90d  │ │
│  │  Public access: Blocked                       │ │
│  └───────────────────────────────────────────────┘ │
│                                                     │
│  ┌───────────────────────────────────────────────┐ │
│  │  IAM User: instacart-spark                    │ │
│  │  Policy: S3LakehouseAccess                    │ │
│  │  Actions: ListBucket, GetObject, PutObject,   │ │
│  │           DeleteObject                        │ │
│  └───────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Local Development (Docker Compose)

```
┌──────────────────────────────────────────────┐
│              Docker Compose                   │
│                                              │
│  ┌──────────┐  ┌──────────────┐  ┌────────┐ │
│  │ MongoDB   │  │ Warehouse API│  │ Mongo  │ │
│  │ :27017    │◄─│ :8000        │  │Express │ │
│  │           │  │              │  │ :8081  │ │
│  │ metadata  │  │ DuckDB+Cache │  │  (UI)  │ │
│  │ catalog   │  │ + sqlglot    │  │        │ │
│  └──────────┘  └──────────────┘  └────────┘ │
│       ▲               │                      │
│       │               │  reads Iceberg       │
│       │               ▼                      │
│       │          ┌──────────┐                │
│       │          │  AWS S3  │                │
│       │          │ (Iceberg)│                │
│       │          └──────────┘                │
│       │               │                      │
│       └───────────────┘                      │
│         writes metadata                      │
└──────────────────────────────────────────────┘
```

---

## 9. Key Design Decisions & Trade-offs

| Decision | Why | Trade-off |
|----------|-----|-----------|
| **Spark OSS instead of Databricks** | Resume already has Bosch + Azure + Databricks. No need to repeat. | Must manage Spark cluster manually on EC2 for production |
| **MongoDB as catalog** | Document model fits metadata well (flexible schema). Docker-friendly. | Not a real catalog (no ACID transactions for metadata). Fine for MVP |
| **DuckDB as query engine** | Reads Iceberg natively. In-process (no server). Blazing fast for analytics. | Single-node only. Not suitable for concurrent heavy workloads |
| **In-process cache instead of Redis** | MVP simplicity. No extra container. TTL-based expiry. | Not shared across instances. Lost on restart |
| **sqlglot for validation** | AST-based — catches tricks that regex/string matching would miss. | Extra dependency. Parsing overhead (minimal for MVP) |
| **dbt-spark via Thrift** | Pure OSS. No Databricks dependency. Standard Spark connection. | Requires Spark Thrift Server running separately |
| **Market basket mining as optional** | Shows ML capability without blocking the core pipeline. | FPGrowth results aren't validated by dbt tests |
| **3-layer medallion** | Industry standard (Bronze/Silver/Gold). Clear separation of concerns. | More storage cost than 2-layer. Worth it for auditability |

---

## 10. File Cross-Reference (Who Imports Whom)

```
config/instacart_config.py
    ▲ ▲ ▲ ▲
    │ │ │ └── pyspark/silver_transformation.py
    │ │ └──── pyspark/bronze_ingestion.py
    │ └────── pyspark/market_basket_mining.py
    └──────── scripts/register_metadata.py

warehouse/engine.py
    ▲
    └── warehouse/main.py (imported as duckdb_engine)

warehouse/metadata.py
    ▲
    ├── warehouse/main.py (imported as metadata_store)
    └── scripts/register_metadata.py (uses MetadataStore directly)

warehouse/sql_validator.py
    ▲
    └── warehouse/main.py (validate_sql called in /query endpoint)

warehouse/models.py
    ▲
    └── warehouse/main.py (QueryRequest, QueryResponse, etc.)

warehouse/cache/memory_cache.py
    ▲
    └── warehouse/engine.py (imports _cache, clear_cache)
```

---

## 11. Quick Start Commands

```bash
# 1. Start infrastructure (MongoDB + API)
docker-compose up -d

# 2. Check API is running
curl http://localhost:8000/health

# 3. List datasets in catalog
curl http://localhost:8000/datasets

# 4. Execute a query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM gold.dim_product LIMIT 10"}'

# 5. View query history
curl http://localhost:8000/history

# 6. View data contract for a table
curl http://localhost:8000/contracts/gold.fct_order_products

# 7. Run tests
python -m pytest warehouse/tests/ -v

# 8. MongoDB web UI
open http://localhost:8081
```

---

## 12. The Portfolio Story

This project demonstrates:

1. **Open Lakehouse Architecture** — Medallion pattern (Bronze/Silver/Gold) with Iceberg on S3, using pure OSS Spark (no Databricks)
2. **Query Gateway Pattern** — API that abstracts the engine: developer thinks it's PostgreSQL, but it's DuckDB + Iceberg + S3
3. **Metadata-Driven Pipeline** — MongoDB as control plane (catalog + contracts + history), Spark reads metadata to know what to process
4. **Data Contracts** — Expectations stored in MongoDB, queryable via API, future Spark-side validation
5. **SQL Security** — AST-based injection protection (sqlglot), not naive regex
6. **Infrastructure as Code** — Terraform for AWS (S3 + IAM), Docker Compose for local dev
7. **CI/CD** — GitLab CI with warehouse tests + conditional dbt tests
8. **ML on Lakehouse** — FPGrowth market basket mining as a bonus differentiator

**What makes it different from a typical "Databricks wrapper"**: You built the compute layer yourself (Spark OSS), you understand every layer of the stack, and you can explain why each component was chosen.
