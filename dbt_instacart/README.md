# Instacart Lakehouse - dbt Project

dbt project for transforming Iceberg Silver layer data into BigQuery dimensional model.

## Project Structure

```
dbt_instacart/
├── models/
│   ├── staging/              # Stage data from Iceberg Silver
│   │   ├── stg_orders.sql
│   │   ├── stg_order_products.sql
│   │   ├── stg_products.sql
│   │   └── schema.yml
│   ├── marts/
│   │   ├── dimensions/       # Dimension tables
│   │   │   ├── dim_user.sql
│   │   │   ├── dim_product.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_aisle.sql
│   │   │   └── dim_department.sql
│   │   ├── facts/           # Fact tables
│   │   │   ├── fct_order_products.sql
│   │   │   └── fct_orders.sql
│   │   ├── analytics/       # Business analytics views
│   │   │   ├── mart_product_performance.sql
│   │   │   ├── mart_user_segments.sql
│   │   │   └── mart_shopping_patterns.sql
│   │   └── schema.yml
│   └── sources.yml          # Iceberg Silver tables as sources
├── macros/                   # Custom SQL macros
├── tests/                    # Custom data tests
├── seeds/                    # Reference data (CSV)
├── snapshots/               # SCD Type 2 snapshots
└── dbt_project.yml          # Project configuration
```

## Setup

### 1. Install dbt
```bash
pip install dbt-core dbt-bigquery
```

### 2. Configure Profile
Create `~/.dbt/profiles.yml`:

```yaml
instacart_lakehouse:
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/service-account-key.json
      project: your-gcp-project-id
      dataset: instacart_lakehouse_dev
      location: US
      threads: 4
      timeout_seconds: 300
    
    prod:
      type: bigquery
      method: service-account
      keyfile: /path/to/service-account-key.json
      project: your-gcp-project-id
      dataset: instacart_lakehouse
      location: US
      threads: 8
      timeout_seconds: 600
  
  target: dev
```

### 3. Test Connection
```bash
dbt debug
```

## Usage

### Run All Models
```bash
# Development
dbt run --target dev

# Production
dbt run --target prod
```

### Run Specific Layers
```bash
# Staging only
dbt run --select staging

# Dimensions only
dbt run --select marts.dimensions

# Facts only
dbt run --select marts.facts

# Analytics only
dbt run --select marts.analytics
```

### Run Tests
```bash
# All tests
dbt test

# Test specific model
dbt test --select dim_user

# Test sources
dbt test --select source:*
```

### Generate Documentation
```bash
# Generate docs
dbt docs generate

# Serve docs locally
dbt docs serve
```

### Build (Run + Test)
```bash
dbt build --target prod
```

## Data Flow

```
Iceberg Silver (GCS)
    ↓
Staging Models (BigQuery Views)
    ↓
Dimensional Model (BigQuery Tables)
    ├── Dimensions (SCD Type 1)
    └── Facts (Partitioned by date)
    ↓
Analytics Marts (BigQuery Views)
```

## Data Lineage

### Sources (Iceberg Silver)
- `silver.orders_enriched` → `stg_orders`
- `silver.order_products_enriched` → `stg_order_products`
- `silver.products_hierarchy` → `stg_products`
- `silver.user_order_summary` → `stg_users`

### Staging → Dimensions
- `stg_orders` + `stg_users` → `dim_user`
- `stg_products` → `dim_product`, `dim_aisle`, `dim_department`
- SQL macro → `dim_date` (date spine)

### Staging → Facts
- `stg_order_products` + dimensions → `fct_order_products`
- `fct_order_products` (aggregated) → `fct_orders`

### Facts → Analytics
- `fct_order_products` + dimensions → `mart_product_performance`
- `dim_user` + `fct_orders` → `mart_user_segments`
- `fct_orders` → `mart_shopping_patterns`

## Key Metrics

### Product Metrics
- Reorder rate by product/aisle/department
- Basket penetration
- Product affinity (frequently bought together)

### User Metrics
- User segmentation (New/Active/Power/Lapsed)
- Lifetime value
- Retention cohorts

### Shopping Pattern Metrics
- Peak hours by day of week
- Average basket size
- Days between orders distribution

## Testing Strategy

### dbt Tests Applied

1. **Schema Tests** (via `schema.yml`)
   - `unique` - Primary keys
   - `not_null` - Required columns
   - `relationships` - Foreign keys
   - `accepted_values` - Enum checks

2. **Custom Tests** (via `tests/`)
   - Reorder rate between 0-1
   - Positive order counts
   - Valid date ranges

3. **Source Freshness** (via `sources.yml`)
   - Check Iceberg Silver tables updated within 24h

## Cost Optimization

### Partitioning
- `fct_order_products` partitioned by `order_date`
- `fct_orders` partitioned by `order_date`

### Clustering
- `fct_order_products` clustered by `user_key`, `product_key`
- `fct_orders` clustered by `user_key`

### Materialization Strategy
- Staging: **Views** (no storage cost)
- Dimensions: **Tables** (small, full refresh)
- Facts: **Tables** (large, partitioned)
- Analytics: **Views** (computed on-demand)

## Development Workflow

1. **Create branch**
   ```bash
   git checkout -b feature/new-mart
   ```

2. **Develop in dev target**
   ```bash
   dbt run --select +my_new_model --target dev
   dbt test --select my_new_model --target dev
   ```

3. **Review changes**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

4. **Deploy to prod**
   ```bash
   dbt run --select +my_new_model --target prod
   dbt test --select my_new_model --target prod
   ```

## Troubleshooting

### Issue: Source table not found
**Solution:** Verify Iceberg Silver tables exist in GCS
```bash
python scripts/validate_iceberg_tables.py --layer silver
```

### Issue: BigQuery permission denied
**Solution:** Grant service account `BigQuery Data Editor` role
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/bigquery.dataEditor"
```

### Issue: dbt compilation error
**Solution:** Check SQL syntax
```bash
dbt compile --select problematic_model
# Check compiled SQL in target/compiled/
```

## Next Steps

1. Implement staging models (read from Iceberg Silver)
2. Implement dimensional model
3. Add dbt tests
4. Create analytical marts
5. Setup dbt Cloud (optional) for scheduling
