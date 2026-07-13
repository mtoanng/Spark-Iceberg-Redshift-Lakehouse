# ✅ CONSOLIDATED CLEANUP & VERIFICATION

**Mục đích:** Tài liệu tổng hợp để verify toàn bộ codebase trước khi đọc chi tiết

**Thời gian tạo:** 2026-07-13

---

## 📋 QUICK STATUS SUMMARY

### **✅ Đã Complete:**
- [x] **Phase 1-3:** Refactor cấu trúc folder (2-plane separation)
- [x] **Phase 4:** Bronze/Silver Glue Jobs
- [x] **Phase 5:** Gold layer (dbt models)
- [x] **Phase 6:** ML training + MongoDB recommendations
- [x] **Bug Fixes:** All 8 critical bugs fixed
- [x] **Documentation:** 4 core MD files + Reading Guide
- [x] **Cleanup:** Old code archived, docs consolidated

### **📁 Final Structure:**
```
Spark-Iceberg-DuckDB-Lakehouse/
├── etl/                          # ETL PLANE
│   ├── glue_jobs/                # AWS Glue Jobs (Bronze + Silver)
│   ├── dbt_project/              # dbt-glue (Gold layer)
│   ├── ml/                       # XGBoost training + recommendation generation
│   └── dags/                     # Airflow orchestration
│
├── warehouse/                    # WAREHOUSE PLANE
│   ├── engine/                   # DuckDB query engine
│   ├── parser/                   # SQL validator (AST-based)
│   ├── api/                      # FastAPI endpoints
│   └── recommendation_store.py   # MongoDB client
│
├── terraform/                    # Infrastructure as Code
├── docker-compose.yml            # Local dev environment
└── *.md                          # Core documentation (4 files)
```

---

## 🐛 CRITICAL BUGS - VERIFICATION CHECKLIST

### **Bug #1: fct_order_products missing user_id**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: etl/dbt_project/models/marts/facts/fct_order_products.sql
# Line ~24-26 should have:
o.user_id,
```

**Why Critical:**
- `mart_user_product_features` needs `user_id` to join
- Without it, ML features can't be computed per user

**Test:**
```sql
-- Should return user_id column
SELECT user_id FROM glue_catalog.gold.fct_order_products LIMIT 1;
```

---

### **Bug #2: mart_user_product_features incorrect target labels**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: etl/dbt_project/models/marts/ml/mart_user_product_features.sql
# Lines ~60-68 should have separate train_labels CTE:
train_labels AS (
    SELECT 
        user_id,
        product_id,
        reordered as target_reordered
    FROM {{ ref('fct_order_products') }}
    WHERE eval_set = 'train'
),
```

**Why Critical:**
- Before fix: All rows had target=0 or 1 (CASE ELSE 0)
- After fix: Only train samples have target, rest are NULL
- NULL rows = prediction samples for generating recommendations
- Wrong fix breaks train/test split

**Test:**
```sql
-- Should have mix of NULL and non-NULL targets
SELECT 
    COUNT(*) as total_rows,
    COUNT(target_reordered) as training_rows,
    COUNT(*) - COUNT(target_reordered) as prediction_rows
FROM glue_catalog.gold.mart_user_product_features;
-- Expect: ~300K training, ~1.7M prediction
```

---

### **Bug #3: SQL validator keyword blacklist (false positives)**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: warehouse/parser/sql_validator.py
# Line ~45 should use parse() PLURAL:
statements = sqlglot.parse(sql, dialect="duckdb")

# Line ~50 should check statement count:
if len(statements) != 1:
    return False, "Only single statement allowed"

# Line ~55 should check AST root:
if tree.key not in ("select", "with"):
    return False, ...
```

**Why Critical:**
- Before fix: Substring matching ("drop" in sql) blocks "SELECT created_at"
- After fix: AST-based, only checks root node type
- Multi-statement detection: parse() returns list, check len != 1

**Test:**
```python
# Should PASS (no false positive)
validate_sql("SELECT created_at FROM orders")  # → (True, "Valid")
validate_sql("SELECT updated_at FROM products")  # → (True, "Valid")

# Should BLOCK
validate_sql("SELECT 1; DROP TABLE x;")  # → (False, "multi-statement")
validate_sql("DROP TABLE orders")  # → (False, "Only SELECT/WITH allowed")
```

---

### **Bug #4: POST /query using query param instead of JSON body**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: warehouse/api/main.py
# Line ~30-35 should have Pydantic model:
class QueryRequest(BaseModel):
    sql: str
    params: Optional[List] = None

# Line ~95 should use model:
def execute_query(request: QueryRequest):
```

**Why Critical:**
- Query params don't support complex SQL with special chars
- JSON body is standard REST practice for POST requests
- Enables proper request validation with Pydantic

**Test:**
```bash
# Should work
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM gold.fct_order_products LIMIT 10"}'
```

---

### **Bug #5: duckdb_engine AttributeError on _use_fallback**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: warehouse/engine/duckdb_engine.py
# Line ~56 should initialize BEFORE if/else:
# CRITICAL FIX: Initialize fallback flag BEFORE any branching
self._use_fallback = False

# Then on line ~70 AFTER if block:
if use_glue_catalog and account_id:
    try:
        self._attach_glue_catalog(account_id, region)
        self._use_fallback = False  # <-- Can set after init
    except:
        self._use_fallback = True   # <-- Can set after init
```

**Why Critical:**
- Before fix: `_use_fallback` only initialized in except branch
- If ATTACH succeeds, attribute never set → AttributeError on first query
- After fix: Always initialized, then conditionally updated

**Test:**
```python
# Should not raise AttributeError
engine = DuckDBEngine(use_glue_catalog=True, account_id="123456789012")
engine.execute("SELECT 1")  # Works regardless of ATTACH success/failure
```

---

### **Bug #6: MongoDB exposed on public port**
**Status:** ✅ FIXED

**Verify:**
```bash
# Check file: docker-compose.yml
# MongoDB service should have NO ports mapping:
mongodb:
  # ports:
  #   - "27017:27017"  # ← Should be commented out!
```

**Why Critical:**
- MongoDB contains recommendation data (potentially sensitive)
- Should only be accessible via warehouse-api (internal network)
- Public exposure = security vulnerability

**Test:**
```bash
# Should fail (no public access)
mongo --host localhost:27017

# Should work (via warehouse-api)
curl http://localhost:8000/recommendations/12345
```

---

### **Bug #7: Multi-statement SQL injection**
**Status:** ✅ FIXED (covered by Bug #3)

**Verify:**
Same as Bug #3 - `len(statements) != 1` check

**Test:**
```python
# Should block
validate_sql("SELECT * FROM orders; DELETE FROM orders;")
# → (False, "Only single statement allowed")
```

---

### **Bug #8: SQL validator false positives**
**Status:** ✅ FIXED (covered by Bug #3)

**Verify:**
Same as Bug #3 - No substring matching, AST-based only

**Test:**
```python
# Should all PASS
validate_sql("SELECT created_at FROM orders")
validate_sql("SELECT updated_at FROM products")
validate_sql("SELECT dropped_column FROM table")  # "drop" substring OK!
```

---

## 📊 DATA FLOW - COMPLETE MAPPING

### **Layer 1: Bronze (6 tables) - Glue Job**
**File:** `etl/glue_jobs/bronze_ingestion.py`

| CSV File | Bronze Table | Schema | Rows |
|----------|-------------|--------|------|
| orders.csv | bronze.orders | order_id, user_id, ... | 3.4M |
| products.csv | bronze.products | product_id, product_name, ... | 50K |
| aisles.csv | bronze.aisles | aisle_id, aisle | 134 |
| departments.csv | bronze.departments | department_id, department | 21 |
| order_products__prior.csv | bronze.order_products_prior | order_id, product_id, ... | 32M |
| order_products__train.csv | bronze.order_products_train | order_id, product_id, ... | 1.4M |

**Key Pattern:**
```python
# Read CSV → Add metadata → Write Iceberg
df.writeTo("glue_catalog.bronze.orders").using("iceberg").createOrReplace()
```

---

### **Layer 2: Silver (3 tables) - Glue Job**
**File:** `etl/glue_jobs/silver_transformation.py`

| Bronze Source(s) | Silver Table | Transformation |
|------------------|-------------|----------------|
| bronze.orders | silver.orders_enriched | + user_total_orders, is_first_order (Window functions) |
| bronze.order_products_* (UNION) + products + aisles + departments | silver.order_products_enriched | UNION prior+train, join dimensions, dedup, partition by department_id |
| bronze.products + aisles + departments | silver.products_hierarchy | Flatten product → aisle → department hierarchy |

**Key Pattern:**
```python
# Read Bronze → Transform → Write Silver with partitioning
df.writeTo("glue_catalog.silver.order_products_enriched") \
  .partitionedBy("department_id") \
  .using("iceberg") \
  .createOrReplace()
```

---

### **Layer 3: Gold - Staging (5 models) - dbt**
**Files:** `etl/dbt_project/models/staging/*.sql`

| Silver Source | Gold Staging View | Purpose |
|---------------|------------------|---------|
| silver.orders_enriched | gold.stg_orders | Clean orders with user metrics |
| silver.order_products_enriched | gold.stg_order_products | Clean order-product associations |
| silver.products_hierarchy | gold.stg_products | Clean product dimensions |
| bronze.aisles* | gold.stg_aisles | Clean aisle dimensions |
| bronze.departments* | gold.stg_departments | Clean department dimensions |

**Note:** stg_aisles and stg_departments read from **Bronze** (not Silver) because Silver doesn't have separate tables for them.

**Key Pattern:**
```sql
-- dbt staging model
SELECT 
    order_id,
    user_id,
    ...
FROM {{ source('bronze', 'orders') }}  -- Read from Bronze
-- Writes to gold.stg_orders via dbt_project.yml config
```

---

### **Layer 4: Gold - Marts (5 models) - dbt**
**Files:** `etl/dbt_project/models/marts/**/*.sql`

#### **Dimensions (2 models)**
| Staging Sources | Gold Dimension | Grain |
|-----------------|---------------|-------|
| stg_orders | gold.dim_orders | user_id, order_id |
| stg_products + stg_aisles + stg_departments | gold.dim_product | product_id (with full hierarchy) |

#### **Facts (1 model) - CRITICAL**
| Staging Sources | Gold Fact | Grain | Critical Columns |
|-----------------|-----------|-------|-----------------|
| stg_order_products + stg_orders + stg_products | gold.fct_order_products | (order_id, product_id) | ✅ user_id, eval_set |

#### **Analytics (2 models)**
| Fact Source | Gold Analytics | Purpose |
|-------------|---------------|---------|
| fct_order_products | gold.mart_product_reorder_rate | Reorder % per product |
| fct_order_products | gold.mart_department_demand | Orders per department |

#### **ML Features (1 model) - CRITICAL**
| Multiple Sources | Gold ML Mart | Features | Target |
|------------------|--------------|----------|--------|
| fct_order_products + dim_orders | gold.mart_user_product_features | 12 features | ✅ target_reordered (NULL for non-train) |

**Key Pattern:**
```sql
-- dbt mart model (fct_order_products)
SELECT
    o.user_id,        -- ✅ CRITICAL
    op.order_id,
    op.product_id,
    op.reordered,
    o.eval_set,       -- ✅ CRITICAL
    ...
FROM {{ ref('stg_order_products') }} op
INNER JOIN {{ ref('stg_orders') }} o ON op.order_id = o.order_id
```

---

### **Layer 5: ML Pipeline (2 scripts) - Python**
**Files:** `etl/ml/train_reorder_model.py`, `etl/ml/generate_recommendations.py`

| Script | Input | Output |
|--------|-------|--------|
| train_reorder_model.py | gold.mart_user_product_features (WHERE target IS NOT NULL) | model_artifacts/reorder_model.xgb |
| generate_recommendations.py | gold.mart_user_product_features (ALL rows) + model | MongoDB: recommendations collection |

**Data Flow:**
```
Gold ML Mart (2M rows)
├── Training samples (300K rows, target NOT NULL) → XGBoost training
└── All rows (2M) → Predict with model → Top-10 per user → MongoDB
```

**MongoDB Schema:**
```json
{
  "user_id": 12345,
  "products": [
    {"product_id": 101, "product_name": "Banana", "score": 0.92},
    {"product_id": 202, "product_name": "Milk", "score": 0.87}
  ],
  "model_version": "xgboost_v1",
  "generated_at": "2026-07-13T10:30:00Z"
}
```

---

## 🔄 ORCHESTRATION - AIRFLOW DAG

**File:** `etl/dags/instacart_pipeline_dag.py`

**Task Flow:**
```
validate_schema 
  ↓
bronze_ingestion (GlueJobOperator)
  ↓
silver_transformation (GlueJobOperator)
  ↓
dbt_run (BashOperator: dbt run)
  ↓
dbt_test (BashOperator: dbt test)
  ↓
train_reorder_model (BashOperator: python train_reorder_model.py)
  ↓
generate_recommendations (BashOperator: python generate_recommendations.py)
  ↓
verify_recommendations (PythonOperator)
```

**Schedule:** `@weekly` (dataset is static snapshot)

**Glue Job Names (must match Terraform):**
- `instacart-lakehouse-bronze-ingestion`
- `instacart-lakehouse-silver-transformation`

---

## 🏗️ INFRASTRUCTURE - TERRAFORM

**Key Resources:**

| File | Resource | Matches |
|------|----------|---------|
| glue_catalog.tf | aws_glue_catalog_database | Database: instacart_lakehouse_{env} |
| glue_jobs.tf | aws_glue_job.bronze_ingestion | Job name: instacart-lakehouse-bronze-ingestion |
| glue_jobs.tf | aws_glue_job.silver_transformation | Job name: instacart-lakehouse-silver-transformation |
| s3.tf | aws_s3_bucket | Bucket for lakehouse data |
| iam.tf | aws_iam_role.glue_service_role | Permissions for Glue Jobs |

**Terraform → Airflow Mapping:**
```
Terraform Job Name                        Airflow DAG Task
─────────────────────────────────────────────────────────────
instacart-lakehouse-bronze-ingestion   → bronze_ingestion (GlueJobOperator)
instacart-lakehouse-silver-transformation → silver_transformation (GlueJobOperator)
```

---

## 🐳 DOCKER COMPOSE - LOCAL DEV

**File:** `docker-compose.yml`

**Services:**
1. **mongodb** - Recommendation store (INTERNAL ONLY, no port mapping)
2. **warehouse-api** - FastAPI + DuckDB (port 8000)
3. **mongo-express** - MongoDB UI (port 8081, optional)

**Network:** `warehouse-network` (bridge)

**Critical Config:**
```yaml
mongodb:
  # ✅ NO public port exposure
  # ports:
  #   - "27017:27017"  # Commented out!
  environment:
    MONGO_INITDB_DATABASE: instacart_warehouse

warehouse-api:
  ports:
    - "8000:8000"  # ✅ Only API exposed
  environment:
    MONGODB_URI: mongodb://admin:admin123@mongodb:27017/  # ✅ Internal network
```

---

## ✅ PRE-READING VERIFICATION

**Before reading codebase, verify:**

### **1. Structure Check**
```bash
# Should have these key folders
ls -la etl/glue_jobs/
ls -la etl/dbt_project/models/
ls -la etl/ml/
ls -la warehouse/engine/
ls -la warehouse/parser/
ls -la warehouse/api/
ls -la terraform/
```

### **2. Critical Files Exist**
```bash
# Glue Jobs
ls etl/glue_jobs/bronze_ingestion.py
ls etl/glue_jobs/silver_transformation.py

# dbt models
ls etl/dbt_project/models/marts/facts/fct_order_products.sql
ls etl/dbt_project/models/marts/ml/mart_user_product_features.sql

# Warehouse
ls warehouse/parser/sql_validator.py
ls warehouse/engine/duckdb_engine.py
ls warehouse/api/main.py

# Orchestration
ls etl/dags/instacart_pipeline_dag.py

# Infrastructure
ls terraform/glue_jobs.tf
ls docker-compose.yml
```

### **3. Documentation Complete**
```bash
# Should have these 4 core docs
ls README.md
ls REFACTOR_BLUEPRINT.md
ls DEPLOYMENT_GUIDE.md
ls DEVELOPMENT.md
ls CODEBASE_READING_GUIDE.md
```

### **4. Old Code Cleaned Up**
```bash
# These should NOT exist (moved to archive)
! ls dags/ 2>/dev/null
! ls pyspark/ 2>/dev/null
! ls dbt_instacart/ 2>/dev/null
! ls warehouse/engine.py 2>/dev/null  # (moved to warehouse/engine/duckdb_engine.py)
```

---

## 📚 NEXT STEPS

**You are here:** ✅ All bugs fixed, codebase ready to read

**What to do:**

1. **Read:** Follow [CODEBASE_READING_GUIDE.md](../CODEBASE_READING_GUIDE.md) (2-3 hours)
   - Layer 1: Architecture (30 min)
   - Layer 2: Data Flow (30 min)
   - Layer 3: ETL Plane (45 min)
   - Layer 4: Warehouse Plane (30 min)
   - Layer 5: Infrastructure (15 min)
   - Layer 6: Orchestration (10 min)

2. **Verify Understanding:** Complete verification checklist in reading guide

3. **Deploy:** Follow [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md) when ready

4. **Develop:** Reference [DEVELOPMENT.md](../DEVELOPMENT.md) for coding standards

---

**Status:** ✅ Ready to read codebase

**Last Updated:** 2026-07-13

**Next Action:** Open `CODEBASE_READING_GUIDE.md` and start from Layer 1

