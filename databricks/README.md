# Databricks Configuration for Instacart Lakehouse

**Hybrid Cloud Architecture:**
- **Storage:** AWS S3 (Iceberg tables)
- **Compute:** Databricks Community Edition (FREE)
- **Serving:** GCP BigQuery (dbt)

---

## 🎯 Why Databricks Community Edition?

✅ **Free Tier** - No cost for development  
✅ **Managed Spark** - No local setup needed  
✅ **Iceberg Support** - Compatible with Apache Iceberg  
✅ **S3 Integration** - Native support for AWS S3  
✅ **Resume Value** - Shows Databricks + multi-cloud experience

---

## 🚀 Setup Steps

### 1. Create Databricks Account
1. Visit https://community.cloud.databricks.com/
2. Sign up with email (free)
3. Verify account

### 2. Create Cluster
1. Go to **Compute** → **Create Cluster**
2. Configuration:
   - **Cluster Name:** `instacart-lakehouse`
   - **Runtime:** `13.3 LTS` (or latest LTS)
   - **Node Type:** `Standard_DS3_v2` (Community default)
   - **Terminate after:** `120 minutes` of inactivity
3. Click **Create Cluster**

### 3. Install Libraries on Cluster
1. Go to your cluster → **Libraries** → **Install New**
2. Install these Maven libraries:

```
org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0
org.apache.hadoop:hadoop-aws:3.3.4
```

3. Wait for installation to complete (status: **Installed**)

### 4. Configure AWS Credentials

#### Option A: Via Databricks Secrets (Recommended)
```python
# In Databricks notebook:
dbutils.secrets.put(scope="aws", key="access_key_id", value="your_access_key")
dbutils.secrets.put(scope="aws", key="secret_access_key", value="your_secret_key")
```

#### Option B: Via Environment Variables
```python
# In notebook cell:
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key'
```

### 5. Test S3 Connection

Create a test notebook:

```python
# Test S3 access
spark.conf.set("spark.hadoop.fs.s3a.access.key", "your_access_key")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "your_secret_key")

# Read test file
df = spark.read.csv("s3a://your-bucket/raw/instacart/departments.csv", header=True)
df.show()
```

### 5. Upload Project Files
```python
# Create project structure in DBFS
dbutils.fs.mkdirs("/instacart/pyspark/")
dbutils.fs.mkdirs("/instacart/config/")

# Upload files via UI:
# Data → DBFS → Upload to /instacart/
# Upload: pyspark/*.py, config/*.py
```

---

## 📝 Running Scripts

### Option 1: Via Notebooks (Recommended for Development)

**Create Bronze Notebook:**
```python
# Cell 1: Import modules
import sys
sys.path.insert(0, '/dbfs/instacart')

# Cell 2: Run bronze ingestion
%run /dbfs/instacart/pyspark/bronze_ingestion.py
```

**Create Silver Notebook:**
```python
%run /dbfs/instacart/pyspark/silver_transformation.py
```

**Create Quality Checks Notebook:**
```python
%run /dbfs/instacart/pyspark/data_quality_checks.py
```

### Option 2: Via Databricks Jobs (for Automation)

**Create Job Definition:**
```json
{
  "name": "Instacart Bronze Ingestion",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "spark_python_task": {
        "python_file": "dbfs:/instacart/pyspark/bronze_ingestion.py"
      },
      "existing_cluster_id": "your-cluster-id"
    }
  ]
}
```

**Create via Databricks CLI:**
```bash
# Install CLI
pip install databricks-cli

# Configure
databricks configure --token
# Enter host: https://community.cloud.databricks.com
# Enter token: (generate from User Settings → Access Tokens)

# Create job
databricks jobs create --json-file databricks/bronze_job.json

# Run job
databricks jobs run-now --job-id <job-id>

# Check status
databricks runs get --run-id <run-id>
```

---

## 🔧 Configuration Updates

### Spark Configuration for GCS Access
```python
# In your notebook or script:
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Instacart-Lakehouse") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/dbfs/credentials/gcp-key.json") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "gs://your-bucket/bronze/") \
    .getOrCreate()
```

### Environment Variables in Databricks
```python
# Set environment variables in notebook
import os
os.environ['GCP_PROJECT_ID'] = 'your-project-id'
os.environ['GCS_BUCKET'] = 'your-bucket-name'
```

---

## 📊 Monitoring & Debugging

### View Spark UI
1. Go to your cluster
2. Click **Spark UI** tab
3. View stages, tasks, storage

### Check Logs
```python
# View driver logs
dbutils.fs.head("dbfs:/cluster-logs/<cluster-id>/driver/stderr")

# View executor logs
dbutils.fs.ls("dbfs:/cluster-logs/<cluster-id>/executor/")
```

### Debug GCS Access
```python
# Test GCS connection
df = spark.read.csv("gs://your-bucket/raw/instacart/orders.csv", header=True)
print(df.count())
```

---

## 💰 Cost Optimization (Community Edition)

Databricks Community Edition is **free** but has limits:
- **1 cluster** at a time
- **15GB RAM** max
- **Auto-terminate** after 2 hours inactivity

**Best Practices:**
1. ✅ Terminate cluster when not in use
2. ✅ Use `.cache()` for reused DataFrames
3. ✅ Partition data appropriately
4. ✅ Use Iceberg for efficient storage

---

## 🔄 Alternative: Databricks Repos (Git Integration)

**Setup Repos:**
```bash
# 1. In Databricks: Workspace → Repos → Add Repo
# 2. Enter your GitHub repo URL
# 3. Clone repository

# Now you can run notebooks directly from Git:
%run ./pyspark/bronze_ingestion
```

---

## 🎓 Databricks Community Edition Limitations

| Feature | Community | Full Databricks |
|---------|-----------|-----------------|
| Cost | Free | Paid |
| Clusters | 1 concurrent | Unlimited |
| RAM | 15GB | Unlimited |
| Auto-termination | 2 hours | Configurable |
| Jobs API | Limited | Full |
| Unity Catalog | ❌ | ✅ |
| Delta Live Tables | ❌ | ✅ |

**Verdict:** Perfect for development and portfolio projects! ✅

---

## 📚 Resources

- [Databricks Community Edition](https://community.cloud.databricks.com/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [Apache Iceberg on Databricks](https://docs.databricks.com/lakehouse/iceberg.html)
- [GCS Connector for Spark](https://github.com/GoogleCloudDataproc/hadoop-connectors)

---

## ✅ Quick Start Checklist

- [ ] Create Databricks Community account
- [ ] Create cluster (Runtime 13.3 LTS)
- [ ] Install Iceberg + GCS libraries
- [ ] Upload GCP service account key to DBFS
- [ ] Upload project scripts to DBFS
- [ ] Test GCS connection
- [ ] Run Bronze ingestion notebook
- [ ] Run Silver transformation notebook
- [ ] Run data quality checks notebook

---

**Ready to run on Databricks!** 🚀
