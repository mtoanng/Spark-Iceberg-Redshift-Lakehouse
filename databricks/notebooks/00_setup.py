# Databricks notebook source
# MAGIC %md
# MAGIC # Instacart Lakehouse - Setup & Configuration
# MAGIC 
# MAGIC This notebook sets up the Databricks environment for the Instacart pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Upload GCP Service Account Key

# COMMAND ----------

# Create credentials directory
dbutils.fs.mkdirs("/credentials/")

# Option 1: Upload via UI
# - Go to Data → DBFS → Upload
# - Upload your gcp-key.json to /credentials/

# Option 2: Via code (paste your JSON content below)
import json

# TODO: Replace with your actual service account key
gcp_key = {
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "your-sa@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}

# Uncomment to write key
# dbutils.fs.put("/credentials/gcp-key.json", json.dumps(gcp_key), overwrite=True)

# Verify upload
display(dbutils.fs.ls("/credentials/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Project Structure in DBFS

# COMMAND ----------

# Create directories
dbutils.fs.mkdirs("/instacart/pyspark/")
dbutils.fs.mkdirs("/instacart/config/")
dbutils.fs.mkdirs("/instacart/scripts/")

print("✅ Project directories created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Set Environment Variables

# COMMAND ----------

import os

# Set environment variables
os.environ['GCP_PROJECT_ID'] = 'your-project-id'  # TODO: Update
os.environ['GCS_BUCKET'] = 'your-bucket-name'     # TODO: Update
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/dbfs/credentials/gcp-key.json'

print("✅ Environment variables set")
print(f"Project: {os.environ['GCP_PROJECT_ID']}")
print(f"Bucket: {os.environ['GCS_BUCKET']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test GCS Connection

# COMMAND ----------

from pyspark.sql import SparkSession

# Test GCS read
try:
    # Read a small test file (adjust path as needed)
    df = spark.read.csv(
        f"gs://{os.environ['GCS_BUCKET']}/raw/instacart/departments.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"✅ GCS connection successful!")
    print(f"Row count: {df.count()}")
    df.show(5)
    
except Exception as e:
    print(f"❌ GCS connection failed: {str(e)}")
    print("\nTroubleshooting:")
    print("1. Verify GCP service account key is uploaded correctly")
    print("2. Check GCS bucket name is correct")
    print("3. Ensure service account has Storage Object Viewer role")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Iceberg Libraries

# COMMAND ----------

# Check if Iceberg is available
try:
    from pyspark.sql import SparkSession
    
    spark_test = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", f"gs://{os.environ['GCS_BUCKET']}/bronze/") \
        .getOrCreate()
    
    print("✅ Iceberg libraries loaded successfully")
    
except Exception as e:
    print(f"❌ Iceberg setup failed: {str(e)}")
    print("\nInstall libraries:")
    print("1. Go to your cluster → Libraries")
    print("2. Install: org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0")
    print("3. Install: com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC 
# MAGIC You can now proceed to run:
# MAGIC - `01_bronze_ingestion` notebook
# MAGIC - `02_silver_transformation` notebook
# MAGIC - `03_data_quality_checks` notebook
