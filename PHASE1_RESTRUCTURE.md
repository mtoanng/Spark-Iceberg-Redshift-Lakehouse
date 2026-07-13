# PHASE 1: Repository Restructure - STATUS

**Branch:** `refactor/glue-recommendations`  
**Started:** 2026-07-13  
**Status:** ✅ In Progress

---

## ✅ Completed Steps

### 1. Git Branch Created
- [x] Backup main branch
- [x] Create `refactor/glue-recommendations` branch
- [x] Commit: "docs: add refactor blueprint and update project status"

### 2. Directory Structure Created

```
✅ etl/
   ✅ dags/
   ✅ glue_jobs/
   ✅ dbt_project/
   ✅ ml/
      ✅ model_artifacts/

✅ warehouse/
   ✅ api/
   ✅ engine/
   ✅ parser/
   ✅ sdk/
      ✅ python/
   ✅ data/
   ✅ tests/
```

---

## ✅ Completed: Core Files Created

### 3. ETL Plane Files (Adapted from existing Databricks code)
- [x] etl/glue_jobs/bronze_ingestion.py (adapted from pyspark/bronze_ingestion.py)
- [x] etl/glue_jobs/silver_transformation.py (adapted from pyspark/silver_transformation.py)
- [x] etl/glue_jobs/__init__.py
- [ ] etl/dags/instacart_pipeline_dag.py (TODO: Phase 4)
- [ ] etl/ml/train_reorder_model.py (TODO: Phase 6)
- [ ] etl/ml/generate_recommendations.py (TODO: Phase 6)

### 4. Warehouse Plane Files (Created from blueprint)
- [x] warehouse/api/main.py (with Pydantic QueryRequest fix)
- [x] warehouse/engine/duckdb_engine.py (with _use_fallback init fix)
- [x] warehouse/parser/sql_validator.py (AST-based, no keyword blacklist)
- [x] warehouse/recommendation_store.py
- [x] warehouse/sdk/python/warehouse_client.py
- [x] warehouse/__init__.py
- [x] warehouse/api/__init__.py
- [x] warehouse/engine/__init__.py
- [x] warehouse/parser/__init__.py
- [x] warehouse/sdk/__init__.py
- [x] warehouse/sdk/python/__init__.py

### 5. Bug Fixes Applied to Blueprint
- [x] fct_order_products.sql: Added user_id and eval_set columns
- [x] mart_user_product_features.sql: Removed MODE() function (user_favorite_dow)
- [x] FEATURE_COLS arrays: Removed user_favorite_dow
- [x] SQL validator: Pure AST-based (no keyword blacklist)
- [x] DuckDB engine: Initialize _use_fallback before branching
- [x] FastAPI: Use Pydantic QueryRequest for JSON body

## ⏳ Next Steps

### 6. Update Configuration Files
- [ ] Update docker-compose.yml (MongoDB internal only, use env vars)
- [ ] Update .env.example (add MONGO_USER, MONGO_PASSWORD)

### 7. ✅ COMPLETED: Create dbt Models (Phase 2)
- [x] Copy existing dbt_instacart/ to etl/dbt_project/
- [x] Update dbt_project.yml for dbt-glue
- [x] Update profiles.yml for AWS Glue interactive sessions
- [x] Update sources.yml (iceberg → glue_catalog)
- [x] Update all staging models (iceberg_bronze/silver → glue_bronze/silver)
- [x] Create corrected fct_order_products.sql (with user_id, eval_set)
- [x] Create mart_user_product_features.sql (with train_labels CTE fix, no MODE())
- [x] Create models/marts/ml/ directory
- [x] Create comprehensive README.md

## ✅ PHASE 2 COMPLETE

**Total Models:** 10 dbt models
- 5 staging views
- 2 dimension tables
- 1 fact table
- 2 analytics views
- 1 ML feature table (NEW)

**Bug Fixes Verified:**
- ✅ fct_order_products includes user_id, eval_set
- ✅ mart_user_product_features uses train_labels CTE with LEFT JOIN
- ✅ MODE() function removed (user_favorite_dow)
- ✅ All catalog references updated to glue_catalog

---

## ⏳ Next Steps

### 8. Phase 3: AWS Glue + Terraform (4-6 hours)

---

## 📋 Acceptance Criteria

- [ ] Repo builds without import errors
- [ ] Directory structure matches blueprint
- [ ] Git tracking all necessary files
- [ ] Ready for Phase 2 (dbt models)

---

**Next Action:** Create skeleton files with proper imports
