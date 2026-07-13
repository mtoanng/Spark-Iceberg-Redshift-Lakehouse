# 🚀 Quick Reference Card

**One-page guide to the entire project**

---

## 📋 Essential Commands

### **Setup (First Time)**
```bash
# 1. Clone and install
git clone <repo>
cd Spark-Iceberg-DuckDB-Lakehouse
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 3. Provision AWS
cd terraform && terraform apply

# 4. Start local services
docker-compose up -d
```

### **Run Pipeline**
```bash
# Bronze layer
spark-submit pyspark/bronze_ingestion.py

# Silver layer
spark-submit pyspark/silver_transformation.py

# Gold layer (dbt)
cd dbt_instacart
dbt run --profiles-dir ~/.dbt --target prod
dbt test

# Register metadata
python scripts/register_metadata.py

# Seed metrics
python scripts/seed_metrics.py
```

### **Start Warehouse API**
```bash
cd warehouse
uvicorn main:app --reload --port 8000

# Access docs: http://localhost:8000/docs
```

### **Test Everything**
```bash
# Test metrics API
python scripts/test_metrics_api.py

# Test SQL queries
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM gold.dim_orders"}'

# List metrics
curl http://localhost:8000/metrics
```

---

## 🏗️ Architecture (One-Liner)

**CSV → Spark/Iceberg (Bronze/Silver) → dbt/Iceberg (Gold) → MongoDB (metadata) + DuckDB (queries) → FastAPI → Python SDK**

---

## 📊 Key Numbers

- **Data:** 33M+ records, 6 tables, ~2GB
- **Cost:** $0 (free tiers only)
- **Timeline:** 7 days to complete
- **Code:** ~5000 lines total
- **Metrics:** 6 example business metrics
- **Endpoints:** 11 API endpoints (3 datasets + 8 metrics)

---

## 🔗 Important URLs

```bash
# API docs
http://localhost:8000/docs

# Mongo Express
http://localhost:8081

# dbt docs
cd dbt_instacart && dbt docs serve
http://localhost:8080
```

---

## 📂 Key Files

| File | Purpose |
|------|---------|
| `PROJECT_MASTER.md` | Complete project context |
| `IMPLEMENTATION_SUMMARY.md` | What was implemented |
| `README.md` | Project overview |
| `warehouse/main.py` | FastAPI app |
| `warehouse/metrics_engine.py` | Metrics execution |
| `scripts/seed_metrics.py` | Load example metrics |
| `dbt_instacart/models/` | Gold layer models |

---

## 🐍 Python SDK Quick Start

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# Query data
df = client.query("SELECT * FROM gold.dim_product LIMIT 10")

# List metrics
metrics = client.list_metrics()

# Execute metric
result = client.execute_metric("avg_basket_size_by_hour")
print(result['preview'])

# Execute with parameters
result = client.execute_metric(
    "top_reordered_products",
    parameters={"min_orders": 150, "limit": 5}
)
```

---

## 🎯 Decision Points

**Should I start?**
- ✅ Yes if: Want to learn modern data stack, have 7 days, okay with $0 budget
- ❌ No if: Need production scale immediately, can't use free tiers

**Which path?**
- **Option A:** Execute on AWS (7-day plan in PROJECT_MASTER.md)
- **Option B:** Test locally first (Docker only, no AWS/Databricks yet)

**What's next after core pipeline?**
- Week 1: Core pipeline (Bronze → Gold)
- Week 2: Metrics Store enhancement
- Week 3: Portfolio materials

---

## 💡 Key Design Decisions

| Decision | Reasoning |
|----------|-----------|
| **Iceberg not Delta** | Multi-engine support (Spark + DuckDB) |
| **MongoDB not Postgres** | Flexible schema for nested metadata |
| **DuckDB not Spark** | Faster for small analytical queries |
| **dbt-spark not Airflow** | SQL-based, testable, version controlled |
| **Metrics in MongoDB not YAML** | Self-service, no deployment needed |
| **Free tier only** | Learning project, demonstrate cost awareness |

---

## 🚨 Common Issues

**Issue:** Databricks connection fails  
**Fix:** Check DATABRICKS_HOST and DATABRICKS_TOKEN in .env

**Issue:** MongoDB connection refused  
**Fix:** `docker-compose up -d mongodb`

**Issue:** DuckDB can't read S3  
**Fix:** Check AWS credentials in .env

**Issue:** No metrics found  
**Fix:** Run `python scripts/seed_metrics.py`

**Issue:** dbt connection fails  
**Fix:** Update `~/.dbt/profiles.yml` with Databricks credentials

---

## 📞 Support

**Documentation:**
- Full context: `PROJECT_MASTER.md`
- Implementation details: `IMPLEMENTATION_SUMMARY.md`
- Project overview: `README.md`

**Scripts:**
- Seed metrics: `scripts/seed_metrics.py`
- Test API: `scripts/test_metrics_api.py`
- Register metadata: `scripts/register_metadata.py`

---

## ✅ Checklist for Deployment

### **Pre-deployment:**
- [ ] AWS account created
- [ ] Databricks AWS trial activated (14 days)
- [ ] MongoDB Atlas M0 created
- [ ] Kaggle API token obtained
- [ ] `.env` configured with all credentials
- [ ] Local Docker tested

### **Deployment Day 1-7:**
- [ ] Day 1: Infrastructure setup
- [ ] Day 2: Data upload to S3
- [ ] Day 3: Bronze layer
- [ ] Day 4: Silver layer
- [ ] Day 5: Gold layer (dbt)
- [ ] Day 6: Warehouse API + Metrics
- [ ] Day 7: Documentation + Screenshots

### **Post-deployment:**
- [ ] Export Databricks notebooks
- [ ] Screenshot all dashboards
- [ ] Git commit everything
- [ ] Create presentation deck
- [ ] Update LinkedIn profile
- [ ] Prepare demo video

---

## 🎓 Interview Prep (30-second elevator pitch)

> "I built an end-to-end data lakehouse processing 33 million Instacart records through a Medallion architecture—Bronze, Silver, Gold layers—using Apache Iceberg on AWS S3.
>
> The interesting part is the warehouse layer: I used MongoDB as a metadata catalog, but also implemented a Metrics Store pattern where business logic is stored as data, not YAML files. This enables self-service analytics—analysts can register and execute metrics via API without code deployment.
>
> The entire platform runs on free tiers—AWS S3, Databricks trial, MongoDB Atlas—total cost zero dollars. It demonstrates both modern data engineering patterns and cost awareness."

---

**Last Updated:** 2026-07-12  
**Status:** Ready to execute  
**Next:** Choose Option A (AWS) or B (Local) from PROJECT_MASTER.md

