# 📇 QUICK REFERENCE CARD

**Print this or keep it open while reading code**

---

## 🎯 DATA FLOW (1 SENTENCE)

```
CSV → Bronze (Glue) → Silver (Glue) → Gold (dbt) → XGBoost → MongoDB
```

---

## 📊 LAYER SUMMARY

| Layer | Created By | Schema | Tables | Purpose |
|-------|-----------|--------|--------|---------|
| **Bronze** | Glue Job | bronze | 6 | Raw CSV ingestion |
| **Silver** | Glue Job | silver | 3 | Enriched + partitioned |
| **Gold** | dbt | gold | 10 | Dimensional + ML features |

---

## 🔑 CRITICAL TABLES (Must Remember)

### **fct_order_products**
```sql
-- Grain: (order_id, product_id)
-- MUST have: user_id, eval_set
-- Used by: mart_user_product_features
```

### **mart_user_product_features**
```sql
-- 12 features for XGBoost
-- target_reordered:
--   NOT NULL = training samples (eval_set='train')
--   NULL = prediction samples
-- MUST use: train_labels CTE with LEFT JOIN
```

---

## 🐛 8 CRITICAL BUGS (Quick Check)

| # | File | Line | Must Have |
|---|------|------|-----------|
| 1 | fct_order_products.sql | ~24 | `o.user_id,` |
| 2 | mart_user_product_features.sql | ~60 | `train_labels AS (... WHERE eval_set='train')` |
| 3 | sql_validator.py | ~45 | `statements = sqlglot.parse()` (plural) |
| 3 | sql_validator.py | ~50 | `if len(statements) != 1:` |
| 4 | main.py | ~30 | `class QueryRequest(BaseModel)` |
| 5 | duckdb_engine.py | ~56 | `self._use_fallback = False` (before if) |
| 6 | docker-compose.yml | mongodb | NO `ports:` mapping |

---

## 📁 FILE LOCATIONS (Quick Find)

```
BRONZE/SILVER GLUE JOBS:
  etl/glue_jobs/bronze_ingestion.py
  etl/glue_jobs/silver_transformation.py

DBT CRITICAL MODELS:
  etl/dbt_project/models/marts/facts/fct_order_products.sql
  etl/dbt_project/models/marts/ml/mart_user_product_features.sql

WAREHOUSE SECURITY:
  warehouse/parser/sql_validator.py
  warehouse/engine/duckdb_engine.py
  warehouse/api/main.py

ORCHESTRATION:
  etl/dags/instacart_pipeline_dag.py

INFRASTRUCTURE:
  terraform/glue_jobs.tf
  docker-compose.yml
```

---

## 🔐 SECURITY PRINCIPLES

```
1. AST-based validation (no substring matching)
2. Multi-statement blocked (len(statements) != 1)
3. Whitelist only (SELECT/WITH)
4. MongoDB internal only (no public port)
5. Pydantic validation (type safety)
```

---

## 🎯 VERIFICATION QUERIES

```sql
-- Check Bug #1: user_id exists
SELECT user_id FROM glue_catalog.gold.fct_order_products LIMIT 1;

-- Check Bug #2: NULL targets exist
SELECT 
    COUNT(*) as total,
    COUNT(target_reordered) as training,
    COUNT(*) - COUNT(target_reordered) as prediction
FROM glue_catalog.gold.mart_user_product_features;

-- Check Gold tables exist
SHOW TABLES FROM glue_catalog.gold;
```

---

## 🔢 ROW COUNTS (Estimates)

| Table | Rows | Note |
|-------|------|------|
| bronze.order_products_prior | 32M | Historical |
| silver.order_products_enriched | 33M | UNION prior+train |
| gold.fct_order_products | 33M | With user_id |
| gold.mart_user_product_features | 2M | 12 features |
| - training samples | 300K | target NOT NULL |
| - prediction samples | 1.7M | target IS NULL |
| mongodb.recommendations | 200K | Top-10 per user |

---

## 🎓 COLUMNS TO REMEMBER

### **fct_order_products**
```
✅ user_id          (Bug #1 fix - needed for join)
✅ eval_set         (train/test split)
✅ order_id         (grain)
✅ product_id       (grain)
✅ reordered        (target for individual orders)
```

### **mart_user_product_features**
```
User features (3):
  user_total_orders
  user_avg_days_between_orders
  user_avg_order_hour

Product features (3):
  product_total_orders
  product_reorder_rate
  product_avg_cart_position

User-product features (6):
  user_product_order_count
  user_product_reorder_count
  user_product_avg_cart_position
  user_product_last_order_number
  orders_since_last_purchase
  user_product_reorder_rate

Target (1):
  ✅ target_reordered (NULL = prediction, NOT NULL = training)
```

---

## 🚀 AIRFLOW DAG FLOW

```
validate_schema
  ↓
bronze_ingestion (Glue: instacart-lakehouse-bronze-ingestion)
  ↓
silver_transformation (Glue: instacart-lakehouse-silver-transformation)
  ↓
dbt_run (bash: dbt run --target glue)
  ↓
dbt_test (bash: dbt test)
  ↓
train_reorder_model (bash: python train_reorder_model.py)
  ↓
generate_recommendations (bash: python generate_recommendations.py)
  ↓
verify_recommendations (python)
```

---

## 🏗️ TERRAFORM RESOURCES

```
aws_glue_catalog_database.main
  └─ instacart_lakehouse_{environment}

aws_glue_job.bronze_ingestion
  └─ instacart-lakehouse-bronze-ingestion

aws_glue_job.silver_transformation
  └─ instacart-lakehouse-silver-transformation

aws_s3_bucket.lakehouse
  └─ Folders: raw/, warehouse/, temp/, spark-logs/
```

---

## 🐳 DOCKER SERVICES

```
mongodb (27017)          ← INTERNAL ONLY (no port mapping)
  ↓
warehouse-api (8000)     ← PUBLIC (FastAPI + DuckDB)
  ↓
mongo-express (8081)     ← OPTIONAL (UI for dev)
```

---

## 📦 TECH STACK CHEATSHEET

| Component | Tech | File/Folder |
|-----------|------|-------------|
| Storage | S3 + Iceberg | (AWS) |
| Catalog | Glue Catalog | terraform/glue_catalog.tf |
| Bronze/Silver | AWS Glue (Spark) | etl/glue_jobs/ |
| Gold | dbt-glue | etl/dbt_project/ |
| ML | XGBoost | etl/ml/ |
| Orchestration | Airflow | etl/dags/ |
| Query Engine | DuckDB | warehouse/engine/ |
| SQL Validation | sqlglot | warehouse/parser/ |
| API | FastAPI | warehouse/api/ |
| Recommendations | MongoDB | warehouse/recommendation_store.py |
| IaC | Terraform | terraform/ |
| Local Dev | Docker Compose | docker-compose.yml |

---

## ✅ SELF-TEST QUESTIONS

**Can you answer these without looking?**

1. Which Glue Job creates Bronze tables?
   → `bronze_ingestion.py`

2. Which dbt model has user_id column that was missing (Bug #1)?
   → `fct_order_products.sql`

3. Why does mart_user_product_features use train_labels CTE?
   → To get NULL target for prediction samples (not in eval_set='train')

4. Why does SQL validator use parse() not parse_one()?
   → parse() returns list of ALL statements, can detect multi-statement injection

5. Why must _use_fallback initialize before if/else?
   → If ATTACH succeeds, attribute never set → AttributeError

6. Why is MongoDB not exposed on port 27017?
   → Security - should only be internal, accessed via warehouse-api

7. How many features in mart_user_product_features?
   → 12 features (3 user + 3 product + 6 user-product)

8. Where are Gold tables created?
   → By dbt models (not Glue Jobs!)

---

## 🔗 QUICK LINKS

**Read First:**
- [BẮT_ĐẦU_ĐỌC_Ở_ĐÂY.md](../BẮT_ĐẦU_ĐỌC_Ở_ĐÂY.md) - Vietnamese guide
- [README.md](../README.md) - Project overview

**Deep Dive:**
- [CODEBASE_READING_GUIDE.md](../CODEBASE_READING_GUIDE.md) - 6 layers, 2-3 hours
- [ARCHITECTURE_VISUAL.md](./ARCHITECTURE_VISUAL.md) - Visual diagrams

**Reference:**
- [DEVELOPMENT.md](../DEVELOPMENT.md) - Coding standards + bug details
- [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md) - 14-step deployment
- [CONSOLIDATED_CLEANUP.md](./CONSOLIDATED_CLEANUP.md) - Bug verification

---

## 💡 REMEMBER

```
✅ 2-plane separation: ETL (write) vs Warehouse (read)
✅ Data flow: CSV → Bronze → Silver → Gold → ML → MongoDB
✅ Critical files: fct_order_products, mart_user_product_features, sql_validator
✅ 8 critical bugs: ALL must remain fixed
✅ Security: AST-based validation, no multi-statement, MongoDB internal
```

---

**Print this card and keep it visible while reading code!**

