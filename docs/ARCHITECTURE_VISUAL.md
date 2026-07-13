# 🏗️ ARCHITECTURE VISUAL REFERENCE

**Quick visual guide to understand the complete system**

---

## 🔄 END-TO-END DATA FLOW

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RAW DATA (S3)                                   │
│  CSV Files: orders, products, aisles, departments, order_products_*     │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ AWS Glue Job
                                 │ bronze_ingestion.py
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER (Iceberg)                             │
│  Schema: bronze.*                                                        │
│  Tables: orders, products, aisles, departments,                          │
│          order_products_prior, order_products_train                      │
│  Format: Iceberg (S3 + Glue Catalog)                                    │
│  Rows: ~37M total (6 tables)                                            │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ AWS Glue Job
                                 │ silver_transformation.py
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER (Iceberg)                             │
│  Schema: silver.*                                                        │
│  Tables:                                                                 │
│    • orders_enriched (+ user metrics, is_first_order)                   │
│    • order_products_enriched (UNION prior+train, partitioned)           │
│    • products_hierarchy (flattened dimensions)                          │
│  Rows: ~34M total (3 tables)                                            │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ dbt-glue
                                 │ dbt run --target glue
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER (Iceberg)                              │
│  Schema: gold.*                                                          │
│  Models:                                                                 │
│    Staging (5):                                                          │
│      • stg_orders, stg_order_products, stg_products                     │
│      • stg_aisles, stg_departments                                      │
│    Dimensions (2):                                                       │
│      • dim_orders (user + order attrs)                                  │
│      • dim_product (product + aisle + department)                       │
│    Facts (1):                                                            │
│      • fct_order_products (grain: order_id, product_id)                 │
│        ✅ Has: user_id, eval_set (CRITICAL!)                            │
│    Analytics (2):                                                        │
│      • mart_product_reorder_rate                                        │
│      • mart_department_demand                                           │
│    ML (1):                                                               │
│      • mart_user_product_features (12 features)                         │
│        ✅ Target: target_reordered (NULL for non-train)                 │
│  Rows: ~2M in ML mart                                                   │
└────────────────┬────────────────────────────────┬───────────────────────┘
                 │                                │
                 │ XGBoost Training               │ DuckDB Query Engine
                 │ train_reorder_model.py         │ + SQL Validator (AST)
                 ▼                                ▼
┌──────────────────────────────┐    ┌────────────────────────────────────┐
│   MODEL ARTIFACTS            │    │   WAREHOUSE API (FastAPI)          │
│   reorder_model.xgb          │    │   Endpoints:                       │
│   (XGBoost binary)           │    │     • POST /query                  │
│                              │    │     • GET /recommendations/{id}    │
└───────────────┬──────────────┘    │     • GET /schema/{table}          │
                │                   └────────────────────────────────────┘
                │ Generate Recommendations
                │ generate_recommendations.py
                │ (Predict reorder probability)
                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     MONGODB (Recommendations)                            │
│  Database: instacart_warehouse                                           │
│  Collection: recommendations                                             │
│  Document Schema:                                                        │
│    {                                                                     │
│      "user_id": 12345,                                                   │
│      "products": [                                                       │
│        {"product_id": 101, "product_name": "Banana", "score": 0.92},   │
│        {"product_id": 202, "product_name": "Milk", "score": 0.87}      │
│      ],                                                                  │
│      "model_version": "xgboost_v1",                                     │
│      "generated_at": "2026-07-13T10:30:00Z"                             │
│    }                                                                     │
│  Access: ✅ Internal only (no public port)                              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🏢 2-PLANE ARCHITECTURE

```
┌───────────────────────────────────────────────────────────────────────┐
│                          ETL PLANE                                     │
│  Folder: etl/                                                          │
│  Purpose: DATA PROCESSING (Write Data)                                │
│                                                                        │
│  Components:                                                           │
│    1. glue_jobs/                                                       │
│       • bronze_ingestion.py        → CSV to Bronze                    │
│       • silver_transformation.py   → Bronze to Silver                 │
│                                                                        │
│    2. dbt_project/                                                     │
│       • models/staging/            → Silver to Gold staging           │
│       • models/marts/              → Gold dimensions + facts + ML     │
│                                                                        │
│    3. ml/                                                              │
│       • train_reorder_model.py     → XGBoost training                 │
│       • generate_recommendations.py → MongoDB bulk insert             │
│                                                                        │
│    4. dags/                                                            │
│       • instacart_pipeline_dag.py  → Airflow orchestration            │
│                                                                        │
│  Tech Stack: AWS Glue, dbt-glue, XGBoost, Airflow                     │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                       WAREHOUSE PLANE                                  │
│  Folder: warehouse/                                                    │
│  Purpose: DATA SERVING (Read Data)                                    │
│                                                                        │
│  Components:                                                           │
│    1. engine/                                                          │
│       • duckdb_engine.py           → Query engine (Glue Catalog)      │
│                                                                        │
│    2. parser/                                                          │
│       • sql_validator.py           → AST-based security validation    │
│                                                                        │
│    3. api/                                                             │
│       • main.py                    → FastAPI endpoints                │
│                                                                        │
│    4. (root)                                                           │
│       • recommendation_store.py    → MongoDB client                   │
│                                                                        │
│  Tech Stack: DuckDB, FastAPI, MongoDB, sqlglot                        │
└───────────────────────────────────────────────────────────────────────┘

                            ┌──────────────┐
                            │ SHARED DATA  │
                            │ (S3 Iceberg) │
                            └──────────────┘
                               ↑        ↓
                    ETL writes │        │ Warehouse reads
                               │        │
```

---

## 🔐 SECURITY LAYERS

```
┌─────────────────────────────────────────────────────────────────────┐
│                    USER REQUEST                                      │
│  POST /query {"sql": "SELECT * FROM orders; DROP TABLE x;"}         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 1: FASTAPI PYDANTIC VALIDATION                                │
│  • Request body must be valid JSON                                   │
│  • Must match QueryRequest schema                                    │
│  • sql field required (string)                                       │
│  ✅ Pass: {"sql": "..."} is valid JSON                               │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 2: AST-BASED SQL VALIDATION (sqlglot)                         │
│  File: warehouse/parser/sql_validator.py                             │
│                                                                       │
│  Step 1: Parse ALL statements                                        │
│    statements = sqlglot.parse(sql, dialect="duckdb")                 │
│                                                                       │
│  Step 2: Check statement count                                       │
│    if len(statements) != 1:                                          │
│        ❌ Block: "Multi-statement not allowed"                       │
│                                                                       │
│  Step 3: Check AST root node                                         │
│    tree = statements[0]                                              │
│    if tree.key not in ("select", "with"):                            │
│        ❌ Block: "Only SELECT/WITH allowed"                          │
│                                                                       │
│  ✅ Pass: Single SELECT statement with valid syntax                  │
│  ❌ Block: "SELECT 1; DROP TABLE x;" (multi-statement detected!)    │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 3: DUCKDB READ-ONLY EXECUTION                                 │
│  File: warehouse/engine/duckdb_engine.py                             │
│  • Execute query in thread-safe manner                               │
│  • Return results as dict                                            │
│  ✅ Query executed successfully                                      │
└─────────────────────────────────────────────────────────────────────┘

KEY SECURITY PRINCIPLES:
✅ AST-based validation (no false positives like "SELECT created_at")
✅ Multi-statement detection (blocks injection)
✅ Whitelist approach (only SELECT/WITH allowed)
✅ MongoDB internal only (no public port)
✅ Pydantic validation (type safety)
```

---

## 🔢 TABLE ROW COUNTS (Estimated)

```
Layer          Table                           Rows        Note
─────────────────────────────────────────────────────────────────────
BRONZE         orders                          3.4M        All orders
BRONZE         products                        50K         Product catalog
BRONZE         aisles                          134         Aisle dimensions
BRONZE         departments                     21          Department dimensions
BRONZE         order_products_prior            32M         Historical orders
BRONZE         order_products_train            1.4M        Training set
               ─────────────────────────────────────────
               TOTAL BRONZE                    37M+

SILVER         orders_enriched                 3.4M        + user metrics
SILVER         order_products_enriched         33M         UNION prior+train
SILVER         products_hierarchy              50K         Flattened hierarchy
               ─────────────────────────────────────────
               TOTAL SILVER                    ~37M

GOLD           fct_order_products              33M         ✅ Has user_id, eval_set
GOLD           dim_orders                      3.4M        User + order attrs
GOLD           dim_product                     50K         Product + hierarchy
GOLD           mart_user_product_features      2M          ✅ 12 features
               - Training samples              300K        target NOT NULL
               - Prediction samples            1.7M        target IS NULL
               ─────────────────────────────────────────
               TOTAL GOLD                      ~38M

MONGODB        recommendations                 200K        Top-10 per user
               - users with recommendations    ~20K        Active users
               - products per user             10          Top-10 reorder prob
```

---

## 🎯 CRITICAL DEPENDENCIES

```
fct_order_products (CRITICAL TABLE)
    ↓
    ├─ user_id column (Bug #1 fix)
    │  └─ Needed by: mart_user_product_features (JOIN key)
    │
    └─ eval_set column (Bug #2 related)
       └─ Used to filter: WHERE eval_set = 'train' for labels

mart_user_product_features (CRITICAL TABLE)
    ↓
    ├─ train_labels CTE (Bug #2 fix)
    │  └─ SELECT FROM fct_order_products WHERE eval_set = 'train'
    │  └─ LEFT JOIN to get NULL for non-training samples
    │
    ├─ 12 features
    │  └─ User features: total_orders, avg_days_between, avg_hour
    │  └─ Product features: total_orders, reorder_rate, avg_position
    │  └─ User-product features: order_count, reorder_count, ...
    │
    └─ target_reordered
       ├─ NOT NULL → Training samples (~300K)
       └─ IS NULL → Prediction samples (~1.7M)

sql_validator.py (CRITICAL SECURITY)
    ↓
    ├─ sqlglot.parse() PLURAL (Bug #3 fix)
    │  └─ Returns list of ALL statements
    │  └─ Block if len(statements) != 1
    │
    └─ AST-based validation
       └─ Check tree.key in ("select", "with")
       └─ No substring matching (no false positives)

duckdb_engine.py (CRITICAL RELIABILITY)
    ↓
    └─ self._use_fallback = False (Bug #5 fix)
       └─ Initialize BEFORE if/else branching
       └─ Prevents AttributeError on success path

docker-compose.yml (CRITICAL SECURITY)
    ↓
    └─ MongoDB NO port mapping (Bug #6 fix)
       └─ Internal network only
       └─ Accessed via warehouse-api (port 8000)
```

---

## 🚀 DEPLOYMENT SEQUENCE

```
STEP 1: INFRASTRUCTURE
┌──────────────────────────────────────┐
│ terraform apply                       │
│ → S3 bucket                           │
│ → Glue Catalog database              │
│ → Glue Jobs (bronze, silver)         │
│ → IAM roles                           │
└──────────────────────────────────────┘
              ↓
STEP 2: RAW DATA UPLOAD
┌──────────────────────────────────────┐
│ aws s3 sync data/ s3://bucket/raw/   │
│ → CSV files uploaded                 │
└──────────────────────────────────────┘
              ↓
STEP 3: BRONZE INGESTION
┌──────────────────────────────────────┐
│ AWS Glue Job: bronze_ingestion       │
│ → 6 Bronze tables created            │
│ → ~37M rows ingested                 │
└──────────────────────────────────────┘
              ↓
STEP 4: SILVER TRANSFORMATION
┌──────────────────────────────────────┐
│ AWS Glue Job: silver_transformation  │
│ → 3 Silver tables created            │
│ → Enrichment + partitioning          │
└──────────────────────────────────────┘
              ↓
STEP 5: GOLD LAYER (dbt)
┌──────────────────────────────────────┐
│ dbt run --target glue                │
│ → 5 staging models                   │
│ → 5 mart models                      │
│ → ✅ fct_order_products (user_id)   │
│ → ✅ mart_user_product_features      │
└──────────────────────────────────────┘
              ↓
STEP 6: ML TRAINING
┌──────────────────────────────────────┐
│ python train_reorder_model.py       │
│ → Query: WHERE target IS NOT NULL   │
│ → ~300K training samples             │
│ → Model: reorder_model.xgb          │
│ → AUC: 0.80+                         │
└──────────────────────────────────────┘
              ↓
STEP 7: GENERATE RECOMMENDATIONS
┌──────────────────────────────────────┐
│ python generate_recommendations.py   │
│ → Query: ALL rows (~2M)              │
│ → Predict reorder probability        │
│ → Top-10 per user → MongoDB          │
│ → ~200K recommendations              │
└──────────────────────────────────────┘
              ↓
STEP 8: START WAREHOUSE API
┌──────────────────────────────────────┐
│ docker-compose up -d                 │
│ → MongoDB (internal only)            │
│ → warehouse-api (port 8000)          │
│ → mongo-express (port 8081)          │
└──────────────────────────────────────┘
              ↓
STEP 9: VERIFY
┌──────────────────────────────────────┐
│ curl http://localhost:8000/          │
│ curl http://localhost:8000/query     │
│ curl .../recommendations/12345       │
└──────────────────────────────────────┘
              ↓
✅ PRODUCTION READY
```

---

## 📦 TECH STACK SUMMARY

```
CATEGORY              TECHNOLOGY         PURPOSE
─────────────────────────────────────────────────────────────────
Storage               S3 + Iceberg       Lakehouse storage (Bronze/Silver/Gold)
Catalog               AWS Glue Catalog   Metadata + schema registry
Processing            AWS Glue (Spark)   Bronze + Silver ETL
Transformation        dbt-glue           Gold dimensional modeling
ML Training           XGBoost            Reorder prediction model
Orchestration         Apache Airflow     Pipeline scheduling
Query Engine          DuckDB             Ad-hoc SQL queries
API Framework         FastAPI            REST endpoints
Recommendation Store  MongoDB            Pre-computed recommendations
SQL Parsing           sqlglot            AST-based validation
Infrastructure        Terraform          IaC provisioning
Containerization      Docker Compose     Local dev environment
```

---

## 🎓 LEARNING PATH

```
BEGINNER (Never seen the code)
├─ 1. Read: README.md (5 min)
├─ 2. Read: REFACTOR_BLUEPRINT.md (20 min) - Architecture overview
├─ 3. Read: ARCHITECTURE_VISUAL.md (THIS FILE) (15 min) - Visual diagrams
└─ 4. Read: CODEBASE_READING_GUIDE.md Layer 1-2 (1 hour) - Data flow

INTERMEDIATE (Understand architecture)
├─ 5. Read: CODEBASE_READING_GUIDE.md Layer 3-6 (1.5 hours) - Deep dive
├─ 6. Read: Critical files (bronze_ingestion.py, fct_order_products.sql)
├─ 7. Read: DEVELOPMENT.md - Bug list + coding standards
└─ 8. Verify: Run self-tests (sql_validator.py, duckdb_engine.py)

ADVANCED (Ready to deploy)
├─ 9. Read: DEPLOYMENT_GUIDE.md - Step-by-step deployment
├─ 10. Run: terraform plan - Review infrastructure changes
├─ 11. Deploy: Follow deployment guide
└─ 12. Test: Verify all 8 critical bugs remain fixed
```

---

## ✅ VISUAL CHECKLIST

**Use this before reading code:**

```
Architecture Understanding
  ☐ I know what 2-plane separation means
  ☐ I know which layer creates which tables (Bronze/Silver/Gold)
  ☐ I know the difference between Glue Jobs and dbt models
  ☐ I know where ML training happens vs where predictions are stored

Data Flow
  ☐ I can trace a CSV file to its final MongoDB recommendation
  ☐ I know which tables have user_id column
  ☐ I know which table has eval_set column
  ☐ I know why target_reordered can be NULL

Critical Components
  ☐ I know why fct_order_products must have user_id
  ☐ I know why mart_user_product_features uses train_labels CTE
  ☐ I know why SQL validator uses parse() not parse_one()
  ☐ I know why _use_fallback must initialize before branching
  ☐ I know why MongoDB has no public port

Security
  ☐ I know how SQL injection is prevented (AST validation)
  ☐ I know why multi-statement queries are blocked
  ☐ I know why substring matching causes false positives
  ☐ I know why MongoDB is internal only

Deployment
  ☐ I know the order: Terraform → Glue Bronze → Glue Silver → dbt Gold → ML
  ☐ I know which Terraform resources map to which Airflow tasks
  ☐ I know how to verify the deployment worked
```

---

**Status:** ✅ Visual reference complete

**Next Action:** Use this as a reference while reading [CODEBASE_READING_GUIDE.md](../CODEBASE_READING_GUIDE.md)

**Tip:** Keep this file open in a separate window while reading code for quick reference!

