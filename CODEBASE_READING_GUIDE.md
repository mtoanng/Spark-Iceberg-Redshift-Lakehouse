# 📖 CODEBASE READING GUIDE

**Mục đích:** Hiểu toàn bộ codebase và mapping data flow từ đầu đến cuối

**Thời gian:** 2-3 giờ đọc kỹ

---

## 🎯 READING STRATEGY

### **Đọc theo thứ tự này:**

1. **Architecture Overview** (30 min) - Hiểu big picture
2. **Data Flow** (30 min) - Follow data từ CSV → MongoDB
3. **ETL Plane** (45 min) - Glue Jobs + dbt models
4. **Warehouse Plane** (30 min) - API + Query Engine
5. **Infrastructure** (15 min) - Terraform + Docker

---

## 📊 LAYER 1: ARCHITECTURE OVERVIEW (30 min)

### **START HERE:**

#### **1. Read: REFACTOR_BLUEPRINT.md (15 min)**
**Focus on:**
- Section "NEW ARCHITECTURE" - Diagram tổng thể
- Section "2-Plane Repository Structure" - Folder organization
- Section "KEY CHANGES EXPLAINED" - Why AWS Glue, why 2-plane

**Key Takeaway:**
```
ETL Plane (etl/) = Data processing
Warehouse Plane (warehouse/) = Data serving
```

#### **2. Read: dbt_project.yml (10 min)**
**File:** `etl/dbt_project/dbt_project.yml`

**Focus on:**
```yaml
models:
  instacart_lakehouse:
    staging:
      +schema: gold        # ← Staging views go to GOLD schema
    marts:
      +schema: gold        # ← All marts go to GOLD schema
```

**Key Discovery:**
- ✅ **Bronze:** Raw CSV data (Glue Job creates)
- ✅ **Silver:** Cleaned data (Glue Job creates)
- ✅ **Gold:** Dimensional models (dbt creates)
- ⚠️ **Important:** Staging models read from Silver, write to Gold schema!

#### **3. Verify Table Locations:**

| Layer | Created By | Schema | Tables |
|-------|-----------|--------|--------|
| **Bronze** | Glue Job (bronze_ingestion.py) | `bronze` | 6 raw tables |
| **Silver** | Glue Job (silver_transformation.py) | `silver` | 3 enriched tables |
| **Gold** | dbt models | `gold` | 10 models (5 staging + 5 marts) |

**Mental Model:**
```
CSV → [Glue Bronze] → bronze.* 
                    ↓
                [Glue Silver] → silver.* 
                              ↓
                          [dbt] → gold.* (staging + marts)
                                    ↓
                              [XGBoost] → MongoDB
```

---

## 📂 LAYER 2: DATA FLOW MAPPING (30 min)

### **Bảng Mapping Chi Tiết:**

#### **Bronze Layer (6 tables)**
```
CSV File                          → Bronze Table
───────────────────────────────────────────────────────
orders.csv                        → bronze.orders
products.csv                      → bronze.products
aisles.csv                        → bronze.aisles
departments.csv                   → bronze.departments
order_products__prior.csv         → bronze.order_products_prior
order_products__train.csv         → bronze.order_products_train
```

**Read:** `etl/glue_jobs/bronze_ingestion.py` (lines 60-120)

---

#### **Silver Layer (3 tables)**
```
Bronze Table(s)                   → Silver Table
───────────────────────────────────────────────────────
bronze.orders                     → silver.orders_enriched
                                     (+ user metrics, is_first_order)

bronze.order_products_prior       → silver.order_products_enriched
+ bronze.order_products_train        (UNION + join with products)
+ bronze.products
+ bronze.aisles
+ bronze.departments

bronze.products                   → silver.products_hierarchy
+ bronze.aisles                      (flattened hierarchy)
+ bronze.departments
```

**Read:** `etl/glue_jobs/silver_transformation.py` (full file)

---

#### **Gold Layer - Staging (5 models)**
```
Silver Table                      → Gold Staging View
───────────────────────────────────────────────────────
silver.orders_enriched            → gold.stg_orders
silver.order_products_enriched    → gold.stg_order_products
silver.products_hierarchy         → gold.stg_products
silver.aisles                     → gold.stg_aisles (NEW)
silver.departments                → gold.stg_departments (NEW)
```

**Note:** stg_aisles và stg_departments đọc từ Bronze vì Silver không có bảng riêng cho chúng!

**Read:** `etl/dbt_project/models/staging/*.sql` (all 5 files)

---

#### **Gold Layer - Marts (5 models)**

**Dimensions (2):**
```
Staging Views                     → Gold Dimension Table
───────────────────────────────────────────────────────
stg_orders                        → gold.dim_orders
                                     (user-level, order-level attrs)

stg_products                      → gold.dim_product
+ stg_aisles                         (product hierarchy)
+ stg_departments
```

**Facts (1):**
```
Staging Views                     → Gold Fact Table
───────────────────────────────────────────────────────
stg_order_products                → gold.fct_order_products
+ stg_orders                         ✅ Grain: (order_id, product_id)
+ stg_products                       ✅ Has: user_id, eval_set
```

**Analytics (2):**
```
Fact Table                        → Gold Analytics View
───────────────────────────────────────────────────────
fct_order_products                → gold.mart_product_reorder_rate
                                     (reorder % per product)

fct_order_products                → gold.mart_department_demand
                                     (orders per department)
```

**ML Features (1):**
```
Multiple Tables                   → Gold ML Feature Table
───────────────────────────────────────────────────────
fct_order_products                → gold.mart_user_product_features
+ dim_orders                         ✅ 12 features for XGBoost
                                     ✅ Has: target_reordered (NULL for non-train)
```

**Read:** `etl/dbt_project/models/marts/**/*.sql` (all files in subfolders)

---

#### **ML Pipeline (2 scripts)**
```
Gold Table                        → Output
───────────────────────────────────────────────────────
gold.mart_user_product_features   → [train_reorder_model.py]
(WHERE target IS NOT NULL)           → model_artifacts/reorder_model.xgb

gold.mart_user_product_features   → [generate_recommendations.py]
(ALL rows)                           + reorder_model.xgb
                                     → MongoDB: recommendations collection
                                        (user_id → top-10 products)
```

**Read:**
- `etl/ml/train_reorder_model.py` (focus: line ~50-80 for query)
- `etl/ml/generate_recommendations.py` (focus: line ~100-150 for query)

---

## 🔄 LAYER 3: ETL PLANE DEEP DIVE (45 min)

### **3.1 Bronze Ingestion (15 min)**

**File:** `etl/glue_jobs/bronze_ingestion.py`

**Reading Checklist:**

- [ ] **Line 1-30:** Imports
  - ✅ AWS Glue imports (`GlueContext`, `Job`, `getResolvedOptions`)
  - ✅ PySpark functions

- [ ] **Line 32-50:** Glue context setup
  - ✅ `create_glue_context()` function
  - ✅ Iceberg config: `spark.sql.catalog.glue_catalog`
  - ✅ Catalog impl: `org.apache.iceberg.aws.glue.GlueCatalog`

- [ ] **Line 60-100:** Ingest functions (6 functions, 1 per table)
  - ✅ Pattern: Read CSV → Add metadata → Write Iceberg
  - ✅ All write to `glue_catalog.bronze.*`
  - ✅ Note: `writeTo().using("iceberg").createOrReplace()`

- [ ] **Line 200-250:** Main function
  - ✅ `job.init()` at start
  - ✅ Call all 6 ingest functions
  - ✅ `job.commit()` at end

**Key Lines to Remember:**
```python
# Line ~45: Catalog config
spark.conf.set("spark.sql.catalog.glue_catalog", 
               "org.apache.iceberg.spark.SparkCatalog")

# Line ~85: Write pattern
df.writeTo("glue_catalog.bronze.orders") \
  .using("iceberg") \
  .createOrReplace()
```

---

### **3.2 Silver Transformation (15 min)**

**File:** `etl/glue_jobs/silver_transformation.py`

**Reading Checklist:**

- [ ] **Line 50-100:** `create_orders_enriched()`
  - ✅ Read: `bronze.orders`
  - ✅ Add: user_total_orders, is_first_order (Window functions)
  - ✅ Write: `silver.orders_enriched`

- [ ] **Line 110-200:** `create_order_products_enriched()`
  - ✅ UNION: `bronze.order_products_prior` + `bronze.order_products_train`
  - ✅ Join: products + aisles + departments
  - ✅ Dedup: Window function on (order_id, product_id)
  - ✅ Validate: Check orphaned product_ids
  - ✅ Write: `silver.order_products_enriched` PARTITIONED BY department_id

- [ ] **Line 210-250:** `create_products_hierarchy()`
  - ✅ Join: products + aisles + departments
  - ✅ Add: is_organic, is_gluten_free (derived)
  - ✅ Write: `silver.products_hierarchy`

**Key Pattern:**
```python
# Read from Bronze
df = spark.table("glue_catalog.bronze.orders")

# Transform
df_enriched = df.withColumn(...).withColumn(...)

# Write to Silver
df_enriched.writeTo("glue_catalog.silver.orders_enriched") \
           .using("iceberg") \
           .createOrReplace()
```

---

### **3.3 dbt Models (15 min)**

#### **3.3.1 Sources (2 min)**
**File:** `etl/dbt_project/models/sources.yml`

**Check:**
- ✅ `database: bronze` - dbt reads from Bronze schema
- ✅ 6 source tables defined

#### **3.3.2 Staging Models (5 min)**
**Files:** `etl/dbt_project/models/staging/*.sql`

**Pattern:**
```sql
-- stg_orders.sql
SELECT 
    order_id,
    user_id,
    ...
FROM {{ source('bronze', 'orders') }}  -- Read from Bronze
```

**Note:** Staging models read from Bronze (via sources), write to Gold (via config)

#### **3.3.3 Fact Table (5 min) - CRITICAL!**
**File:** `etl/dbt_project/models/marts/facts/fct_order_products.sql`

**Must verify:**
```sql
SELECT
    o.user_id,        -- ✅ MUST HAVE (Bug #1 fix)
    op.order_id,
    op.product_id,
    op.reordered,
    o.eval_set,       -- ✅ MUST HAVE (for train/test split)
    ...
FROM {{ ref('stg_order_products') }} op
INNER JOIN {{ ref('stg_orders') }} o ON op.order_id = o.order_id
INNER JOIN {{ ref('stg_products') }} p ON op.product_id = p.product_id
```

**Why Critical:**
- `user_id` needed for ML features join
- `eval_set` needed to filter training data

#### **3.3.4 ML Feature Mart (3 min) - CRITICAL!**
**File:** `etl/dbt_project/models/marts/ml/mart_user_product_features.sql`

**Must verify train_labels CTE:**
```sql
train_labels AS (
    SELECT 
        user_id,
        product_id,
        reordered as target_reordered
    FROM {{ ref('fct_order_products') }}
    WHERE eval_set = 'train'  -- ✅ Only training samples
),

final_features AS (
    SELECT
        ...
        tl.target_reordered  -- ✅ NULL for non-training
    FROM user_product_stats up
    ...
    LEFT JOIN train_labels tl  -- ✅ MUST be LEFT JOIN
        ON up.user_id = tl.user_id 
        AND up.product_id = tl.product_id
)
```

**Why Critical:** NULL target = prediction samples (not training)

---

## 🏪 LAYER 4: WAREHOUSE PLANE DEEP DIVE (30 min)

### **4.1 DuckDB Engine (10 min)**

**File:** `warehouse/engine/duckdb_engine.py`

**Reading Checklist:**

- [ ] **Line 1-40:** Class definition & __init__
  - ✅ Line ~35: `self._use_fallback = False` BEFORE if/else (Bug #5 fix)
  - ✅ Persistent file: `duckdb.connect(database=db_path)`
  - ✅ Thread lock: `threading.Lock()`

- [ ] **Line 50-80:** `_setup_aws_credentials()`
  - ✅ Create SECRET for S3 access
  - ✅ Use STS assume role

- [ ] **Line 90-110:** `_attach_glue_catalog()`
  - ✅ ATTACH Glue Catalog as `glue_catalog`
  - ✅ Try/except pattern (fallback if fails)

- [ ] **Line 120-140:** `execute()`
  - ✅ Thread-safe (uses lock)
  - ✅ Returns dict with columns, rows, row_count

**Key Pattern:**
```python
def __init__(self, ...):
    # ✅ Initialize BEFORE branching
    self._use_fallback = False
    
    if use_glue_catalog:
        try:
            self._attach_glue_catalog(...)
            self._use_fallback = False  # Success
        except:
            self._use_fallback = True   # Fallback
```

---

### **4.2 SQL Validator (10 min) - CRITICAL!**

**File:** `warehouse/parser/sql_validator.py`

**Reading Checklist:**

- [ ] **Line 1-30:** Imports & function signature
  - ✅ Import `sqlglot`

- [ ] **Line 40-60:** `validate_sql()` function
  - ✅ Line ~45: `statements = sqlglot.parse(sql, dialect="duckdb")`
  - ✅ ⚠️ MUST be `parse()` plural, NOT `parse_one()`
  - ✅ Line ~50: `if len(statements) != 1:` - Block multi-statement
  - ✅ Line ~55: `if tree.key not in ("select", "with"):` - AST check

- [ ] **Line 100-150:** Self-tests in `__main__`
  - ✅ Test: `SELECT created_at FROM orders` → PASS (no false positive)
  - ✅ Test: `SELECT 1; DROP TABLE x;` → FAIL (multi-statement blocked)

**Critical Pattern:**
```python
def validate_sql(sql: str) -> Tuple[bool, str]:
    # ✅ Parse ALL statements (plural!)
    statements = sqlglot.parse(sql, dialect="duckdb")
    
    # ✅ Block multi-statement
    if len(statements) != 1:
        return False, "Only single statement allowed"
    
    tree = statements[0]
    
    # ✅ AST-based check (NO keyword blacklist!)
    if tree.key not in ("select", "with"):
        return False, f"Only SELECT/WITH allowed, got {tree.key.upper()}"
    
    return True, "Valid"
```

**What NOT to do:**
```python
# ❌ WRONG - Substring matching (false positives!)
if "drop" in sql.lower():
    return False  # Fails on "drop_duplicates"
```

---

### **4.3 FastAPI Endpoints (10 min)**

**File:** `warehouse/api/main.py`

**Reading Checklist:**

- [ ] **Line 1-40:** Imports & models
  - ✅ Line ~30: `class QueryRequest(BaseModel):` (Bug #4 fix)
  - ✅ Has `sql: str` and `params: Optional[List]`

- [ ] **Line 50-80:** Initialize engines
  - ✅ `DuckDBEngine` init (singleton)
  - ✅ `RecommendationStore` init (singleton)

- [ ] **Line 90-120:** `POST /query` endpoint
  - ✅ Line ~95: `def execute_query(request: QueryRequest):` (NOT `sql: str`)
  - ✅ Line ~100: `validate_sql(request.sql)` called first
  - ✅ Line ~110: `engine.execute(request.sql)`

- [ ] **Line 130-150:** `GET /recommendations/{user_id}`
  - ✅ Calls `rec_store.get_recommendations(user_id)`
  - ✅ Returns MongoDB document

**Critical Pattern:**
```python
class QueryRequest(BaseModel):  # ✅ Pydantic model
    sql: str
    params: Optional[List] = None

@app.post("/query")
def execute_query(request: QueryRequest):  # ✅ JSON body
    is_valid, message = validate_sql(request.sql)  # ✅ Validate first
    
    if not is_valid:
        raise HTTPException(400, detail=f"Invalid SQL: {message}")
    
    result = engine.execute(request.sql, request.params)
    return QueryResponse(**result)
```

---

## 🏗️ LAYER 5: INFRASTRUCTURE (15 min)

### **5.1 Terraform (10 min)**

**Files to scan:**

#### **main.tf**
- ✅ Provider: AWS
- ✅ Region configuration

#### **s3.tf**
- ✅ S3 bucket for lakehouse
- ✅ Folders: raw/, warehouse/, temp/, spark-logs/

#### **glue_catalog.tf**
- ✅ Database: `instacart_lakehouse_{environment}`
- ✅ Location: S3 bucket

#### **glue_jobs.tf** - IMPORTANT!
- ✅ `aws_glue_job.bronze_ingestion`
  - Name: `instacart-lakehouse-bronze-ingestion` (matches DAG!)
  - Script: S3 upload from `etl/glue_jobs/bronze_ingestion.py`
- ✅ `aws_glue_job.silver_transformation`
  - Name: `instacart-lakehouse-silver-transformation` (matches DAG!)
  - Script: S3 upload from `etl/glue_jobs/silver_transformation.py`

#### **iam.tf**
- ✅ Glue service role
- ✅ S3 permissions
- ✅ Glue Data Catalog permissions

**Key Mapping:**
```
Terraform Job Name                 → DAG Reference
──────────────────────────────────────────────────────────
instacart-lakehouse-bronze-ingestion    
  → airflow DAG task: load_bronze (GlueJobOperator)

instacart-lakehouse-silver-transformation
  → airflow DAG task: transform_silver (GlueJobOperator)
```

---

### **5.2 Docker Compose (5 min)**

**File:** `docker-compose.yml`

**Check:**

- [ ] **MongoDB service**
  - ✅ NO `ports:` mapping (Bug #6 fix - internal only!)
  - ✅ Database: `instacart_warehouse`
  - ✅ Credentials: admin/admin123 (demo only)

- [ ] **warehouse-api service**
  - ✅ Port: 8000 exposed
  - ✅ Env vars: MONGODB_URI, AWS_*, USE_GLUE_CATALOG

- [ ] **mongo-express service** (optional UI)
  - ✅ Port: 8081 exposed (for dev convenience)

**Critical:**
```yaml
mongodb:
  # ✅ NO PORT MAPPING (internal only)
  # ports:
  #   - "27017:27017"  # Commented out!
  environment:
    MONGO_INITDB_DATABASE: instacart_warehouse
```

---

## 🔗 LAYER 6: ORCHESTRATION (10 min)

### **6.1 Airflow DAG**

**File:** `etl/dags/instacart_pipeline_dag.py`

**Reading Checklist:**

- [ ] **Line 1-50:** Imports & DAG definition
  - ✅ `GlueJobOperator` for Glue Jobs
  - ✅ `BashOperator` for dbt & ML

- [ ] **Line 60-250:** Task definitions (8 tasks)

**Task Flow:**
```python
validate_schema 
  >> load_bronze              # GlueJobOperator
  >> transform_silver         # GlueJobOperator
  >> dbt_run                  # BashOperator
  >> dbt_test                 # BashOperator
  >> train_reorder_model      # BashOperator
  >> generate_recommendations # BashOperator
  >> verify_recommendations   # PythonOperator
```

**Task Details:**

| Task | Type | Command/Job |
|------|------|-------------|
| `load_bronze` | GlueJobOperator | `instacart-lakehouse-bronze-ingestion` |
| `transform_silver` | GlueJobOperator | `instacart-lakehouse-silver-transformation` |
| `dbt_run` | BashOperator | `dbt run --target glue` |
| `dbt_test` | BashOperator | `dbt test --target glue` |
| `train_reorder_model` | BashOperator | `python etl/ml/train_reorder_model.py` |
| `generate_recommendations` | BashOperator | `python etl/ml/generate_recommendations.py` |

---

## ✅ VERIFICATION CHECKLIST

After reading, verify you understand:

### **Data Flow**
- [ ] Tôi biết CSV nào tạo Bronze table nào
- [ ] Tôi biết Bronze nào tạo Silver nào
- [ ] Tôi biết Silver nào tạo Gold staging nào
- [ ] Tôi biết Gold staging nào tạo Gold mart nào
- [ ] Tôi biết Gold mart nào feed vào ML

### **Table Schemas**
- [ ] Tôi biết `fct_order_products` có những cột gì (đặc biệt user_id, eval_set)
- [ ] Tôi biết `mart_user_product_features` có 12 features gì
- [ ] Tôi biết `target_reordered` khi nào NULL, khi nào NOT NULL

### **Critical Bugs Fixed**
- [ ] Tôi biết tại sao `fct_order_products` MUST có `user_id`
- [ ] Tôi biết tại sao `mart_user_product_features` MUST dùng `train_labels` CTE
- [ ] Tôi biết tại sao SQL validator MUST dùng `sqlglot.parse()` plural
- [ ] Tôi biết tại sao POST /query MUST dùng Pydantic model
- [ ] Tôi biết tại sao `_use_fallback` MUST init trước branching
- [ ] Tôi biết tại sao MongoDB MUST không có port mapping

### **Components Mapping**
- [ ] Tôi biết Glue Job nào tạo table nào
- [ ] Tôi biết dbt model nào depend on model nào
- [ ] Tôi biết Terraform resource nào map với file code nào
- [ ] Tôi biết Airflow task nào chạy script nào

---

## 📝 READING ORDER SUMMARY

**Recommended sequence (2-3 hours):**

1. ✅ **REFACTOR_BLUEPRINT.md** - Architecture overview
2. ✅ **bronze_ingestion.py** - CSV → Bronze
3. ✅ **silver_transformation.py** - Bronze → Silver
4. ✅ **dbt models** - Silver → Gold (start with staging, then marts)
5. ✅ **train_reorder_model.py** - Gold → Model
6. ✅ **generate_recommendations.py** - Model → MongoDB
7. ✅ **duckdb_engine.py** - Query engine
8. ✅ **sql_validator.py** - Security
9. ✅ **api/main.py** - API endpoints
10. ✅ **Terraform files** - Infrastructure
11. ✅ **instacart_pipeline_dag.py** - Orchestration

---

## 🎯 AFTER READING

Bạn sẽ có mental model đầy đủ để:
- ✅ Debug issues
- ✅ Add new features
- ✅ Modify transformations
- ✅ Optimize performance
- ✅ Deploy to production

**Next:** Follow [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) to run it!

---

**Good luck với việc đọc code! 📚**
