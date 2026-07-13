# 🚀 BẮT ĐẦU ĐỌC CODEBASE TỪ ĐÂY

**Chào mừng! Đây là điểm khởi đầu để bạn hiểu toàn bộ codebase.**

---

## 📚 TÀI LIỆU ĐỌC THEO THỨ TỰ

### **1️⃣ ĐỌC TRƯỚC (30 phút) - Hiểu Big Picture**

#### **a) README.md** (5 phút)
- Tổng quan dự án
- Tech stack
- Folder structure

#### **b) REFACTOR_BLUEPRINT.md** (15 phút)
- **MỤC TIÊU:** Hiểu kiến trúc 2-plane
- **FOCUS VÀO:**
  - Section "NEW ARCHITECTURE" - Diagram tổng thể
  - Section "2-Plane Repository Structure"
  - Section "KEY CHANGES EXPLAINED"

#### **c) docs/ARCHITECTURE_VISUAL.md** (10 phút)
- **MỤC TIÊU:** Visual diagrams dễ hiểu
- **FOCUS VÀO:**
  - End-to-end data flow (CSV → MongoDB)
  - 2-plane architecture diagram
  - Security layers
  - Critical dependencies

---

### **2️⃣ ĐỌC CHÍNH (2-3 giờ) - Deep Dive**

#### **CODEBASE_READING_GUIDE.md** (Full guide)
**Đọc theo 6 layers:**

**Layer 1: Architecture Overview** (30 phút)
- REFACTOR_BLUEPRINT.md (đã đọc, skim lại)
- `etl/dbt_project/dbt_project.yml` (hiểu schema config)
- Mental model: Bronze → Silver → Gold

**Layer 2: Data Flow Mapping** (30 phút)
- Table mapping chi tiết
- Hiểu table nào tạo ra table nào
- Grain của từng bảng

**Layer 3: ETL Plane Deep Dive** (45 phút)
- `etl/glue_jobs/bronze_ingestion.py` - CSV → Bronze
- `etl/glue_jobs/silver_transformation.py` - Bronze → Silver
- `etl/dbt_project/models/staging/*.sql` - Silver → Gold staging
- `etl/dbt_project/models/marts/**/*.sql` - Gold marts
  - **CRITICAL:** `fct_order_products.sql` (must have user_id)
  - **CRITICAL:** `mart_user_product_features.sql` (train_labels CTE)

**Layer 4: Warehouse Plane Deep Dive** (30 phút)
- `warehouse/engine/duckdb_engine.py` - Query engine
  - **CRITICAL:** `_use_fallback` init before branching
- `warehouse/parser/sql_validator.py` - AST validation
  - **CRITICAL:** `sqlglot.parse()` plural, not `parse_one()`
- `warehouse/api/main.py` - FastAPI endpoints
  - **CRITICAL:** `QueryRequest` Pydantic model

**Layer 5: Infrastructure** (15 phút)
- `terraform/glue_jobs.tf` - Glue Job names
- `docker-compose.yml` - MongoDB internal only
  - **CRITICAL:** No `ports:` mapping for MongoDB

**Layer 6: Orchestration** (10 phút)
- `etl/dags/instacart_pipeline_dag.py` - Airflow DAG
- Task flow: validate → bronze → silver → dbt → ML

---

### **3️⃣ REFERENCE (Khi cần) - Quick Lookup**

#### **a) DEVELOPMENT.md**
- Coding standards
- 8 critical bugs detailed
- Testing guidelines

#### **b) docs/CONSOLIDATED_CLEANUP.md**
- Bug verification checklist
- Quick status summary
- File existence checks

#### **c) DEPLOYMENT_GUIDE.md**
- Khi bạn sẵn sàng deploy
- 14-step deployment sequence
- Verification steps

---

## 🎯 CHIẾN LƯỢC ĐỌC CODE

### **Đọc theo mục đích:**

#### **Mục đích 1: Hiểu data flow (1 giờ)**
**Đọc theo thứ tự:**
1. `bronze_ingestion.py` - Line 60-120 (ingest functions)
2. `silver_transformation.py` - Full file (transformations)
3. `etl/dbt_project/models/staging/*.sql` - Skim all 5 files
4. `etl/dbt_project/models/marts/facts/fct_order_products.sql` - Full file

**Verify understanding:**
```
CSV orders.csv 
  → bronze.orders (Glue Job)
  → silver.orders_enriched (Glue Job)
  → gold.stg_orders (dbt)
  → gold.dim_orders (dbt)
  → gold.fct_order_products (dbt) ← JOIN với stg_order_products
```

---

#### **Mục đích 2: Hiểu ML pipeline (30 phút)**
**Đọc theo thứ tự:**
1. `etl/dbt_project/models/marts/ml/mart_user_product_features.sql` - Full file
   - Focus: train_labels CTE (line ~60-68)
2. `etl/ml/train_reorder_model.py` - Line 50-80 (query)
3. `etl/ml/generate_recommendations.py` - Line 100-150 (query + MongoDB)

**Verify understanding:**
```
gold.mart_user_product_features (2M rows)
├─ Training samples (300K, target NOT NULL)
│  └─ train_reorder_model.py → XGBoost model
│
└─ All samples (2M)
   └─ generate_recommendations.py + model → MongoDB top-10
```

---

#### **Mục đích 3: Hiểu security (30 phút)**
**Đọc theo thứ tự:**
1. `warehouse/parser/sql_validator.py` - Full file + self-tests
2. `warehouse/api/main.py` - Line 30-35 (Pydantic model), Line 90-120 (POST /query)
3. `docker-compose.yml` - MongoDB service (line 5-20)

**Verify understanding:**
```
User request 
  → FastAPI (Pydantic validation)
  → sql_validator.py (AST validation)
    ├─ parse() returns list of ALL statements
    ├─ Block if len(statements) != 1
    └─ Check tree.key in ("select", "with")
  → duckdb_engine.py (execute)
  → Response

MongoDB:
  ✅ NO public port (internal only)
  ✅ Access via warehouse-api only
```

---

#### **Mục đích 4: Hiểu orchestration (20 phút)**
**Đọc theo thứ tự:**
1. `etl/dags/instacart_pipeline_dag.py` - Full file
2. `terraform/glue_jobs.tf` - Job names

**Verify understanding:**
```
Airflow DAG tasks:
1. validate_schema (PythonOperator)
2. bronze_ingestion (GlueJobOperator)
   ├─ Job name: instacart-lakehouse-bronze-ingestion
   └─ Defined in: terraform/glue_jobs.tf
3. silver_transformation (GlueJobOperator)
   ├─ Job name: instacart-lakehouse-silver-transformation
   └─ Defined in: terraform/glue_jobs.tf
4. dbt_run (BashOperator: dbt run --target glue)
5. dbt_test (BashOperator: dbt test)
6. train_reorder_model (BashOperator: python ...)
7. generate_recommendations (BashOperator: python ...)
8. verify_recommendations (PythonOperator)
```

---

## 🐛 8 CRITICAL BUGS - PHẢI NHỚ!

**Trong quá trình đọc, verify từng bug này đã được fix:**

### **Bug #1: fct_order_products thiếu user_id**
**File:** `etl/dbt_project/models/marts/facts/fct_order_products.sql`
**Check:** Line ~24 phải có `o.user_id,`
**Tại sao critical:** mart_user_product_features cần join bằng user_id

### **Bug #2: mart_user_product_features target labels sai**
**File:** `etl/dbt_project/models/marts/ml/mart_user_product_features.sql`
**Check:** Line ~60-68 phải có `train_labels AS (SELECT ... WHERE eval_set = 'train')`
**Tại sao critical:** NULL target = prediction samples, NOT NULL = training samples

### **Bug #3: SQL validator substring matching (false positives)**
**File:** `warehouse/parser/sql_validator.py`
**Check:** 
- Line ~45: `statements = sqlglot.parse()` (plural!)
- Line ~50: `if len(statements) != 1` (block multi-statement)
- Line ~55: `if tree.key not in ("select", "with")` (AST-based)
**Tại sao critical:** Substring matching block "SELECT created_at" (false positive)

### **Bug #4: POST /query dùng query param thay vì JSON body**
**File:** `warehouse/api/main.py`
**Check:** 
- Line ~30: `class QueryRequest(BaseModel)`
- Line ~95: `def execute_query(request: QueryRequest)`
**Tại sao critical:** Query params không support complex SQL

### **Bug #5: duckdb_engine AttributeError on _use_fallback**
**File:** `warehouse/engine/duckdb_engine.py`
**Check:** Line ~56 phải có `self._use_fallback = False` TRƯỚC if/else
**Tại sao critical:** Nếu ATTACH success, attribute không tồn tại → crash

### **Bug #6: MongoDB exposed public port**
**File:** `docker-compose.yml`
**Check:** MongoDB service phải KHÔNG có `ports:` mapping
**Tại sao critical:** Security risk, MongoDB chỉ nên internal

### **Bug #7: Multi-statement SQL injection**
**Status:** Fixed by Bug #3 (len(statements) != 1)

### **Bug #8: SQL validator false positives**
**Status:** Fixed by Bug #3 (AST-based, no substring)

---

## ✅ CHECKLIST SAU KHI ĐỌC XONG

**Verify bạn đã hiểu:**

### **Data Flow**
- [ ] Tôi biết CSV nào tạo Bronze table nào
- [ ] Tôi biết Bronze nào tạo Silver nào
- [ ] Tôi biết Silver nào tạo Gold nào
- [ ] Tôi biết dbt model nào depend on model nào
- [ ] Tôi biết ML pipeline flow (Gold → XGBoost → MongoDB)

### **Critical Tables**
- [ ] Tôi biết `fct_order_products` có những cột gì
  - ✅ Phải có: user_id, eval_set
- [ ] Tôi biết `mart_user_product_features` có 12 features gì
  - ✅ User features: total_orders, avg_days_between, avg_hour
  - ✅ Product features: total_orders, reorder_rate, avg_position
  - ✅ User-product features: order_count, reorder_count, ...
- [ ] Tôi biết `target_reordered` khi nào NULL, khi nào NOT NULL
  - ✅ NOT NULL: training samples (eval_set = 'train')
  - ✅ NULL: prediction samples (for recommendations)

### **Architecture**
- [ ] Tôi hiểu 2-plane separation
  - ✅ ETL plane: etl/ (write data)
  - ✅ Warehouse plane: warehouse/ (read data)
- [ ] Tôi biết tech stack cho từng component
  - ✅ Bronze/Silver: AWS Glue (Spark)
  - ✅ Gold: dbt-glue
  - ✅ ML: XGBoost
  - ✅ Query: DuckDB
  - ✅ API: FastAPI
  - ✅ Recommendations: MongoDB

### **Security**
- [ ] Tôi hiểu AST-based SQL validation
- [ ] Tôi biết tại sao multi-statement bị block
- [ ] Tôi biết tại sao substring matching gây false positive
- [ ] Tôi biết tại sao MongoDB không public

### **8 Critical Bugs**
- [ ] Tôi đã verify cả 8 bugs đều đã fix
- [ ] Tôi hiểu tại sao mỗi bug critical
- [ ] Tôi biết cách test từng bug

---

## 🚀 SAU KHI ĐỌC XONG

### **Nếu muốn deploy:**
→ Đọc `DEPLOYMENT_GUIDE.md`

### **Nếu muốn develop:**
→ Đọc `DEVELOPMENT.md`

### **Nếu có câu hỏi về kiến trúc:**
→ Đọc lại `REFACTOR_BLUEPRINT.md`

### **Nếu cần reference nhanh:**
→ Dùng `docs/ARCHITECTURE_VISUAL.md` (diagrams)
→ Dùng `docs/CONSOLIDATED_CLEANUP.md` (checklist)

---

## 💡 TIPS ĐỌC CODE HIỆU QUẢ

### **Tip 1: Đọc top-down, verify bottom-up**
- Đọc từ high-level (architecture) xuống low-level (code details)
- Verify từ low-level (unit tests) lên high-level (integration)

### **Tip 2: Follow data, not files**
- Trace 1 record từ CSV đến MongoDB
- Hiểu transformations ở mỗi layer

### **Tip 3: Focus on "why", not just "what"**
- Đừng chỉ đọc code làm gì
- Hiểu tại sao phải làm vậy (critical bugs explain why)

### **Tip 4: Use visual diagrams**
- Keep `docs/ARCHITECTURE_VISUAL.md` open khi đọc code
- Map code → diagram → understanding

### **Tip 5: Verify bằng SQL queries**
```sql
-- Verify Bug #1 fix
SELECT user_id FROM glue_catalog.gold.fct_order_products LIMIT 1;

-- Verify Bug #2 fix
SELECT 
    COUNT(*) as total,
    COUNT(target_reordered) as training,
    COUNT(*) - COUNT(target_reordered) as prediction
FROM glue_catalog.gold.mart_user_product_features;
```

---

## 📂 FILE STRUCTURE REFERENCE

```
Spark-Iceberg-DuckDB-Lakehouse/
├── BẮT_ĐẦU_ĐỌC_Ở_ĐÂY.md           ← BẠN Ở ĐÂY
├── README.md                        ← Đọc đầu tiên (5 min)
├── REFACTOR_BLUEPRINT.md            ← Đọc thứ hai (15 min)
├── CODEBASE_READING_GUIDE.md        ← Đọc chính (2-3 giờ)
├── DEVELOPMENT.md                   ← Reference (khi develop)
├── DEPLOYMENT_GUIDE.md              ← Reference (khi deploy)
│
├── docs/
│   ├── ARCHITECTURE_VISUAL.md       ← Visual diagrams (10 min)
│   └── CONSOLIDATED_CLEANUP.md      ← Bug checklist
│
├── etl/                             ← ETL PLANE
│   ├── glue_jobs/                   ← Bronze + Silver
│   ├── dbt_project/                 ← Gold
│   ├── ml/                          ← XGBoost
│   └── dags/                        ← Airflow
│
├── warehouse/                       ← WAREHOUSE PLANE
│   ├── engine/                      ← DuckDB
│   ├── parser/                      ← SQL validator
│   ├── api/                         ← FastAPI
│   └── recommendation_store.py      ← MongoDB
│
├── terraform/                       ← Infrastructure
└── docker-compose.yml               ← Local dev
```

---

## 🎓 LEARNING PATH BY EXPERIENCE

### **Nếu bạn chưa từng đọc codebase này:**
1. README.md (5 min)
2. REFACTOR_BLUEPRINT.md (15 min)
3. docs/ARCHITECTURE_VISUAL.md (10 min)
4. CODEBASE_READING_GUIDE.md Layer 1-2 (1 giờ)
→ **Total: 1.5 giờ hiểu big picture**

### **Nếu bạn đã hiểu architecture:**
1. CODEBASE_READING_GUIDE.md Layer 3-6 (1.5 giờ)
2. Đọc critical files (bronze, silver, dbt models)
3. Verify 8 bugs đã fix
→ **Total: 2 giờ hiểu implementation**

### **Nếu bạn sẵn sàng deploy:**
1. DEPLOYMENT_GUIDE.md (30 min đọc)
2. Follow 14 steps (2-4 giờ deploy + verify)
→ **Total: 3-5 giờ production ready**

---

**Status:** ✅ Bạn đã sẵn sàng đọc codebase!

**Next Action:** Mở `README.md` và bắt đầu từ Layer 1

**Good luck! 🚀**

