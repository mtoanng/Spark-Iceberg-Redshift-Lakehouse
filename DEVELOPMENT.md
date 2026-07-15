# 💻 DEVELOPMENT GUIDE

**For developers contributing to or modifying the codebase**

---

## 📂 PROJECT STRUCTURE

```
instacart-lakehouse/
│
├── etl/                          # ETL PLANE
│   ├── dags/
│   │   └── instacart_pipeline_dag.py        # Airflow orchestration
│   ├── glue_jobs/
│   │   ├── bronze_ingestion.py              # CSV → Iceberg
│   │   └── silver_transformation.py         # Clean & enrich
│   ├── dbt_project/
│   │   ├── models/
│   │   │   ├── staging/         (5 models)
│   │   │   └── marts/
│   │   │       ├── dimensions/  (2 models)
│   │   │       ├── facts/       (1 model)
│   │   │       ├── analytics/   (2 models)
│   │   │       └── ml/          (1 model)
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   └── ml/
│       ├── train_reorder_model.py
│       ├── generate_recommendations.py
│       └── model_artifacts/
│
├── warehouse/                    # WAREHOUSE PLANE
│   ├── api/
│   │   └── main.py              # FastAPI endpoints
│   ├── engine/
│   │   └── duckdb_engine.py     # DuckDB + Glue Catalog
│   ├── parser/
│   │   └── sql_validator.py     # SQL security
│   ├── recommendation_store.py  # MongoDB client
│   └── tests/
│
├── terraform/                    # INFRASTRUCTURE
│   ├── main.tf
│   ├── s3.tf
│   ├── glue_catalog.tf
│   ├── glue_jobs.tf
│   └── iam.tf
│
└── docker-compose.yml
```

---

## 🏗️ ARCHITECTURE PRINCIPLES

### **2-Plane Separation**

**ETL Plane (`etl/`):**
- Responsible for: Data ingestion, transformation, model training
- Technologies: AWS Glue, dbt, Python, XGBoost
- Runs on: Serverless (Glue) or scheduled (Airflow)

**Warehouse Plane (`warehouse/`):**
- Responsible for: Query serving, recommendations API
- Technologies: FastAPI, DuckDB, MongoDB
- Runs on: Containers (Docker)

**Key Rule:** ETL writes data, Warehouse reads data. No bidirectional dependencies.

---

### **Data Flow**

```
CSV → Bronze (raw) → Silver (clean) → Gold (modeled) → ML → MongoDB
                                          ↓
                                    DuckDB Query Engine
```

---

## 🔧 DEVELOPMENT SETUP

### **1. Clone & Install**

```bash
git clone <repo-url>
cd instacart-lakehouse

# Install Python dependencies
pip install -r requirements.txt

# Install dbt
pip install dbt-glue

# Install Terraform
# (follow: https://developer.hashicorp.com/terraform/downloads)
```

---

### **2. Local Development Environment**

```bash
# Start local services
docker-compose up -d mongodb

# Set environment variables
export AWS_REGION=us-east-1
export MONGODB_URI=mongodb://admin:admin123@localhost:27017
export USE_GLUE_CATALOG=false  # Use local for dev
export DUCKDB_PATH=warehouse/data/warehouse_dev.db
```

---

### **3. Run Tests**

```bash
# Python syntax check
python -m py_compile etl/**/*.py
python -m py_compile warehouse/**/*.py

# dbt tests
cd etl/dbt_project
dbt parse
dbt compile
dbt test

# Self-tests
python warehouse/parser/sql_validator.py
python warehouse/engine/duckdb_engine.py
python warehouse/recommendation_store.py
python etl/dags/instacart_pipeline_dag.py

# Terraform validation
cd terraform
terraform init
terraform validate
terraform fmt -check
```

---

## 🐛 CRITICAL BUGS (ALREADY FIXED)

These bugs were fixed during refactor. **DO NOT reintroduce:**

### **Bug #1: Missing user_id in fct_order_products**
**File:** `etl/dbt_project/models/marts/facts/fct_order_products.sql`

✅ **MUST HAVE:**
```sql
SELECT
    o.user_id,  -- ✅ REQUIRED for ML join
    ...
```

❌ **DO NOT:**
```sql
SELECT
    -- missing user_id!
    op.order_id,
    ...
```

---

### **Bug #2: Wrong target label logic**
**File:** `etl/dbt_project/models/marts/ml/mart_user_product_features.sql`

✅ **CORRECT (use train_labels CTE):**
```sql
train_labels AS (
    SELECT user_id, product_id, reordered as target_reordered
    FROM {{ ref('fct_order_products') }}
    WHERE eval_set = 'train'  -- ✅ Only training
),

final_features AS (
    ...
    LEFT JOIN train_labels tl  -- ✅ NULL for non-training
        ON up.user_id = tl.user_id
        AND up.product_id = tl.product_id
)
```

❌ **WRONG (never NULL):**
```sql
MAX(CASE WHEN eval_set = 'train' THEN reordered ELSE 0 END) as target
-- ❌ Always 0 or 1, never NULL!
```

---

### **Bug #3: SQL Validator Keyword Blacklist**
**File:** `warehouse/parser/sql_validator.py`

✅ **CORRECT (AST-based):**
```python
statements = sqlglot.parse(sql, dialect="duckdb")  # ✅ plural!

if len(statements) != 1:  # ✅ Block multi-statement
    return False, "Only single statement allowed"

if tree.key not in ("select", "with"):  # ✅ AST check
    return False, f"Only SELECT/WITH allowed"
```

❌ **WRONG (substring matching):**
```python
if "drop" in sql.lower():  # ❌ False positive: "drop_duplicates"
    return False
```

---

### **Bug #4: POST /query using query param**
**File:** `warehouse/api/main.py`

✅ **CORRECT (Pydantic model):**
```python
class QueryRequest(BaseModel):
    sql: str

@app.post("/query")
def execute_query(request: QueryRequest):  # ✅
    ...
```

❌ **WRONG:**
```python
@app.post("/query")
def execute_query(sql: str):  # ❌ Query param, not JSON body!
    ...
```

---

### **Bug #5: _use_fallback not initialized**
**File:** `warehouse/engine/duckdb_engine.py`

✅ **CORRECT:**
```python
def __init__(self, ...):
    self._use_fallback = False  # ✅ Initialize BEFORE branching
    
    if use_glue_catalog:
        try:
            self._attach_glue_catalog(...)
            self._use_fallback = False  # ✅ Set after success
        except:
            self._use_fallback = True
```

❌ **WRONG:**
```python
def __init__(self, ...):
    if use_glue_catalog:
        try:
            self._attach_glue_catalog(...)
            # ❌ _use_fallback not initialized if success!
        except:
            self._use_fallback = True
```

---

### **Bug #6-8: Security Issues**

✅ **MongoDB:** No port mapping (internal only)  
✅ **Multi-statement:** Blocked via `len(statements) != 1`  
✅ **False positives:** No substring matching (AST only)

---

## 🧪 TESTING CHECKLIST

Before committing changes:

### **1. Python Syntax**
```bash
python -m py_compile <your-file>.py
```

### **2. Import Check**
```bash
python -c "from <module> import <class>; print('OK')"
```

### **3. dbt Validation**
```bash
cd etl/dbt_project
dbt compile --select <your-model>
dbt run --select <your-model> --target dev
```

### **4. Terraform Validation**
```bash
cd terraform
terraform fmt
terraform validate
```

### **5. Docker Build**
```bash
docker-compose build warehouse-api
```

### **6. API Test**
```bash
# Start services
docker-compose up -d

# Test endpoint
curl http://localhost:8000/
```

---

## 📝 CODING STANDARDS

### **Python**
- Use type hints: `def func(x: int) -> str:`
- Docstrings for all public functions
- Follow PEP 8
- Max line length: 100 characters

### **SQL (dbt)**
- Use `{{ ref('model') }}` for cross-model references
- Use `{{ source('schema', 'table') }}` for Bronze sources
- Lowercase keywords: `select`, not `SELECT`
- Indent with 4 spaces
- Add comments for complex logic

### **Terraform**
- Use variables for all configurable values
- Add descriptions to all resources
- Run `terraform fmt` before committing

---

## 🔄 CONTRIBUTION WORKFLOW

### **1. Create Feature Branch**
```bash
git checkout -b feature/your-feature-name
```

### **2. Make Changes**
- Follow coding standards
- Add tests
- Update documentation

### **3. Test Locally**
```bash
# Run all checks
./scripts/run_tests.sh  # (if exists)
# Or manually run testing checklist above
```

### **4. Commit**
```bash
git add .
git commit -m "feat: add new feature

- What changed
- Why it changed
- Breaking changes (if any)"
```

### **5. Push & Create PR**
```bash
git push origin feature/your-feature-name
```

---

## 🚨 COMMON PITFALLS

### **1. Forgetting to update imports**
When moving files, update all imports:
```bash
grep -r "from old_path" .
```

### **2. Hardcoding credentials**
Always use environment variables:
```python
# ✅ Good
mongo_uri = os.getenv("MONGODB_URI")

# ❌ Bad
mongo_uri = "mongodb://admin:password@localhost"
```

### **3. Not testing with real data**
Always test with sample of real data, not mocks only

### **4. Ignoring dbt tests**
Run `dbt test` before marking feature complete

---

## 📚 USEFUL COMMANDS

### **dbt**
```bash
dbt run --select model_name         # Run single model
dbt test --select model_name        # Test single model
dbt docs generate && dbt docs serve # View lineage
dbt clean                           # Clean compiled files
```

### **Docker**
```bash
docker-compose up -d                # Start services
docker-compose logs -f service_name # View logs
docker-compose restart service_name # Restart service
docker-compose down                 # Stop all
```

### **AWS**
```bash
# Glue
aws glue start-job-run --job-name <name>
aws glue get-job-run --job-name <name> --run-id <id>

# S3
aws s3 ls s3://bucket/path/
aws s3 cp file.txt s3://bucket/path/

# Athena (query Glue Catalog)
aws athena start-query-execution --query-string "SELECT ..."
```

---

## 🔍 CODE REVIEW CHECKLIST

When reviewing PRs:

- [ ] All tests pass
- [ ] No hardcoded credentials
- [ ] Type hints added
- [ ] Docstrings present
- [ ] No critical bugs reintroduced (see list above)
- [ ] Documentation updated
- [ ] Breaking changes documented

---

## 📖 LEARNING RESOURCES

### **AWS Glue**
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [Iceberg on Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

### **dbt**
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-glue Adapter](https://github.com/aws-samples/dbt-glue)

### **DuckDB**
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)

### **XGBoost**
- [XGBoost Documentation](https://xgboost.readthedocs.io/)

---

## 🆘 GETTING HELP

1. Check **REFACTOR_BLUEPRINT.md** for architecture details
2. Review **DEPLOYMENT_GUIDE.md** for setup issues
3. Check `docs/archive/` for historical context
4. Review Git history: `git log --oneline --graph`

---

**Happy Coding! 🚀**
