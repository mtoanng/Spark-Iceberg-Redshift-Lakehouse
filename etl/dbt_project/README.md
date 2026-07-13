# Instacart Lakehouse - dbt Project

**dbt-glue** transformation project for Instacart Market Basket Analysis

---

## 📋 Overview

This dbt project transforms Silver layer data from AWS Glue Catalog into dimensional Gold layer models for analytics and ML.

**Adapter:** dbt-glue (AWS Glue interactive sessions)  
**Catalog:** AWS Glue Data Catalog  
**File Format:** Apache Iceberg v2

---

## 🏗️ Architecture

```
AWS Glue Catalog (Silver)
         ↓
    dbt-glue Run
         ↓
┌────────────────────────────────────┐
│  Gold Layer (Dimensional Model)   │
├────────────────────────────────────┤
│  Staging (5 views)                 │
│    - stg_orders                    │
│    - stg_order_products            │
│    - stg_products                  │
│    - stg_aisles                    │
│    - stg_departments               │
│                                    │
│  Marts                             │
│    Dimensions (2 tables)           │
│      - dim_orders                  │
│      - dim_products                │
│                                    │
│    Facts (1 table)                 │
│      - fct_order_products          │
│                                    │
│    Analytics (2 views)             │
│      - mart_product_reorder_rate   │
│      - mart_department_demand      │
│                                    │
│    ML Features (1 table) - NEW     │
│      - mart_user_product_features  │
└────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

```bash
# Install dbt-glue
pip install dbt-glue boto3

# Set environment variables
export GLUE_ROLE_ARN="arn:aws:iam::<account>:role/AWSGlueServiceRole-Instacart"
export AWS_REGION="us-east-1"
export DBT_GLUE_STAGING="s3://instacart-lakehouse/dbt-glue-staging/"
```

### Run dbt

```bash
cd etl/dbt_project

# Test connection
dbt debug --profiles-dir . --target glue

# Install dependencies
dbt deps

# Run all models
dbt run --profiles-dir . --target glue

# Run tests
dbt test --profiles-dir . --target glue

# Generate docs
dbt docs generate --profiles-dir . --target glue
dbt docs serve
```

---

## 📊 Models

### Staging Layer (5 models)

Views that clean and standardize Silver data:

- **stg_orders** - Order-level data with user metrics
- **stg_order_products** - Order-product associations
- **stg_products** - Product hierarchy (department → aisle → product)
- **stg_aisles** - Aisle reference
- **stg_departments** - Department reference

### Marts Layer

#### Dimensions (2 tables)

- **dim_orders** - Order dimension with degenerate attributes
- **dim_products** - Product dimension with hierarchy

#### Facts (1 table)

- **fct_order_products** - Fact table at (order_id, product_id) grain
  - **CRITICAL:** Includes `user_id` and `eval_set` columns for ML join
  - Grain: One row per order-product combination
  - ~33M rows

#### Analytics (2 views)

- **mart_product_reorder_rate** - Product reorder metrics
- **mart_department_demand** - Department-level demand patterns

#### ML Features (1 table) - NEW

- **mart_user_product_features** - Feature engineering for reorder prediction
  - 12 features per user-product pair
  - Target column: `target_reordered` (NULL for non-training samples)
  - Used by XGBoost model in Phase 6

---

## 🔧 Configuration

### profiles.yml

```yaml
instacart_lakehouse:
  target: glue
  outputs:
    glue:
      type: glue
      role_arn: "{{ env_var('GLUE_ROLE_ARN') }}"
      region: "{{ env_var('AWS_REGION') }}"
      workers: 2
      worker_type: G.1X
      schema: gold
      database: instacart_lakehouse
      location: "{{ env_var('DBT_GLUE_STAGING') }}"
```

### Sources

- **glue_bronze**: Raw data from CSV ingestion (6 tables)
- **glue_silver**: Enriched and cleaned data (4 tables)

---

## ✅ Critical Changes from Original

**1. Catalog Reference**
- ❌ Old: `iceberg.bronze`, `iceberg.silver`
- ✅ New: `glue_catalog.bronze`, `glue_catalog.silver`

**2. fct_order_products (Bug Fix Applied)**
- ✅ Added `user_id` column (required for ML join)
- ✅ Added `eval_set` column (train/test split)

**3. mart_user_product_features (New Model)**
- ✅ 12 features (removed `user_favorite_dow` - MODE() not supported)
- ✅ Correct target labels via `train_labels` CTE with LEFT JOIN
- ✅ NULL target for non-training samples (not 0!)

---

## 🧪 Testing

```bash
# Run all tests
dbt test --profiles-dir . --target glue

# Test specific model
dbt test --select fct_order_products --profiles-dir . --target glue

# Test sources
dbt test --select source:* --profiles-dir . --target glue
```

### Test Coverage

- Uniqueness: Primary keys
- Not null: Required columns
- Relationships: Foreign keys
- Accepted values: Enums (order_dow, eval_set)

---

## 📝 Development Workflow

1. **Make changes** to SQL models
2. **Test locally** (if possible): `dbt compile --profiles-dir .`
3. **Deploy to Glue**: `dbt run --select <model> --profiles-dir . --target glue`
4. **Run tests**: `dbt test --select <model> --profiles-dir . --target glue`
5. **Document**: Add descriptions to `schema.yml`

---

## 🎯 Lineage

```
glue_bronze.orders
glue_bronze.order_products_prior
glue_bronze.order_products_train
    ↓
glue_silver.orders_enriched
glue_silver.order_products_enriched
    ↓
stg_orders
stg_order_products
    ↓
fct_order_products
    ↓
mart_user_product_features (ML)
```

---

## ⚠️ Known Limitations

1. **dbt-glue adapter**: May have rough edges (check version compatibility)
2. **Glue interactive sessions**: ~2 min cold start time
3. **Iceberg writes**: Ensure Glue Catalog has correct permissions

---

## 📚 Resources

- [dbt-glue Documentation](https://github.com/aws-samples/dbt-glue)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

---

**Maintained by:** Data Engineering Team  
**Last Updated:** 2026-07-13
