# Windows Deployment Guide

**Hướng dẫn deploy trên Windows (đơn giản nhất)**

---

## 📋 Prerequisites

1. **Docker Desktop for Windows**
   - Download: https://www.docker.com/products/docker-desktop/
   - Install và start Docker Desktop
   - Verify: Mở PowerShell, gõ `docker --version`

2. **Python 3.9+**
   - Download: https://www.python.org/downloads/
   - Tick "Add Python to PATH" khi install
   - Verify: `python --version`

3. **AWS CLI**
   - Download: https://aws.amazon.com/cli/
   - Install
   - Verify: `aws --version`

---

## 🚀 Quick Deployment (5 minutes)

### Method 1: Dùng Script Tự Động ✨

```batch
REM 1. Chạy script tự động
RUN.bat

REM Script sẽ tự:
REM - Copy .env.example → .env
REM - Start Docker containers
REM - Show service URLs
```

### Method 2: Làm Thủ Công

```batch
REM 1. Copy .env template
copy .env.example .env

REM 2. Edit .env với notepad
notepad .env

REM 3. Start services
docker-compose up -d

REM 4. Check status
docker-compose ps
```

---

## 🔧 .env Configuration

Edit `.env` file với credentials của bạn:

```bash
# AWS (bắt buộc)
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_REGION=us-east-1
S3_BUCKET=instacart-lakehouse-xxxx

# Databricks (bắt buộc)
DATABRICKS_HOST=https://community.cloud.databricks.com
DATABRICKS_TOKEN=dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
DATABRICKS_CLUSTER_ID=xxxx-xxxxxx-xxxxxxx

# MongoDB (tự động - không cần sửa)
MONGODB_URI=mongodb://admin:admin123@localhost:27017/
MONGODB_DATABASE=instacart_metadata
```

---

## 📊 Check Services

```batch
REM Check all services
docker-compose ps

REM Should show:
REM  instacart-mongodb         running   27017/tcp
REM  instacart-warehouse-api   running   8000/tcp
REM  instacart-mongo-express   running   8081/tcp
```

---

## 🌐 Access Services

1. **Warehouse API**
   - URL: http://localhost:8000
   - Docs: http://localhost:8000/docs
   - Test: Open browser → http://localhost:8000

2. **Mongo Express** (MongoDB Web UI)
   - URL: http://localhost:8081
   - Login: admin / admin
   - View: instacart_metadata → datasets

3. **MongoDB** (Direct Connection)
   - Host: localhost:27017
   - User: admin
   - Pass: admin123

---

## 🧪 Test API

### Using Browser
1. Open: http://localhost:8000/docs
2. Try "GET /datasets"
3. Click "Try it out" → "Execute"

### Using PowerShell
```powershell
# Health check
curl http://localhost:8000/

# List datasets
curl http://localhost:8000/datasets

# Query
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8000/query `
  -ContentType "application/json" `
  -Body '{"sql": "SELECT COUNT(*) FROM gold.dim_user"}'
```

### Using Python
```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")
df = client.query("SELECT * FROM gold.dim_user LIMIT 10")
print(df)
```

---

## 📝 Common Commands

### Start/Stop Services

```batch
REM Start all
docker-compose up -d

REM Stop all
docker-compose stop

REM Restart all
docker-compose restart

REM Stop and remove
docker-compose down
```

### View Logs

```batch
REM All logs
docker-compose logs -f

REM API logs only
docker-compose logs -f warehouse-api

REM MongoDB logs only
docker-compose logs -f mongodb

REM Last 100 lines
docker-compose logs --tail=100 warehouse-api
```

### Rebuild After Code Changes

```batch
REM Rebuild API
docker-compose build warehouse-api

REM Restart with new code
docker-compose up -d warehouse-api
```

---

## 🗄️ MongoDB Management

### Access MongoDB Shell

```batch
REM Open MongoDB shell
docker-compose exec mongodb mongosh -u admin -p admin123 instacart_metadata

REM Then in MongoDB shell:
show collections
db.datasets.find().pretty()
exit
```

### Backup Data

```batch
REM Create backup directory
mkdir backups

REM Backup MongoDB
docker-compose exec mongodb mongodump ^
  -u admin -p admin123 ^
  --authenticationDatabase admin ^
  --db instacart_metadata ^
  --out /tmp/backup

REM Copy to host
docker cp instacart-mongodb:/tmp/backup backups\mongodb-backup
```

---

## 🔄 Full Pipeline Workflow

### 1. Setup Infrastructure (One-time)

```batch
REM Deploy AWS S3 bucket
cd terraform
terraform init
terraform apply
cd ..

REM Note S3 bucket name from output
```

### 2. Download Data

```batch
REM Setup Kaggle API first:
REM 1. Go to kaggle.com → Settings → API → Create New Token
REM 2. Save kaggle.json to: %USERPROFILE%\.kaggle\kaggle.json

REM Download dataset
python scripts\download_kaggle_dataset.py
```

### 3. Upload to S3

```batch
REM Upload data
python scripts\upload_to_s3.py

REM Verify upload
aws s3 ls s3://your-bucket-name/raw/instacart/
```

### 4. Run Pipeline on Databricks

```batch
REM Package code
powershell Compress-Archive -Path pyspark,config -DestinationPath pipeline.zip

REM Upload to Databricks
databricks fs cp pipeline.zip dbfs:/jobs/instacart_pipeline.zip --overwrite

REM Create and run jobs (see DEPLOYMENT.md)
```

### 5. Register Metadata

```batch
REM After dbt completes
python scripts\register_metadata.py

REM Verify in MongoDB
docker-compose exec mongodb mongosh -u admin -p admin123 ^
  --eval "db.datasets.countDocuments()" instacart_metadata
```

---

## 🐛 Troubleshooting

### Issue: Docker not starting

**Solution:**
1. Open Docker Desktop
2. Wait for it to fully start (whale icon in system tray)
3. Try again: `docker-compose up -d`

### Issue: Port 8000 already in use

**Solution:**
```batch
REM Find process using port 8000
netstat -ano | findstr :8000

REM Kill process (replace PID)
taskkill /PID [PID] /F

REM Or change port in docker-compose.yml:
REM   ports: ["8001:8000"]
```

### Issue: MongoDB won't start

**Solution:**
```batch
REM Remove old data
docker-compose down -v

REM Start fresh
docker-compose up -d mongodb

REM Wait 30 seconds
timeout /t 30 /nobreak

REM Check logs
docker-compose logs mongodb
```

### Issue: API can't connect to MongoDB

**Solution:**
```batch
REM Check MongoDB is running
docker-compose ps

REM Test connection
docker-compose exec warehouse-api python -c ^
  "from pymongo import MongoClient; print(MongoClient('mongodb://admin:admin123@mongodb:27017/').server_info())"
```

### Issue: Can't access S3 from API

**Solution:**
```batch
REM Check AWS credentials in .env
notepad .env

REM Verify AWS credentials work
aws s3 ls

REM Restart API
docker-compose restart warehouse-api
```

---

## 🎯 Quick Reference

| Task | Command |
|------|---------|
| **Start services** | `docker-compose up -d` |
| **Stop services** | `docker-compose stop` |
| **View logs** | `docker-compose logs -f` |
| **Restart API** | `docker-compose restart warehouse-api` |
| **MongoDB shell** | `docker-compose exec mongodb mongosh -u admin -p admin123` |
| **Check status** | `docker-compose ps` |
| **Clean up** | `docker-compose down -v` |

---

## 📚 File Structure

```
C:\...\Data-Migration-with-Spark-Airflow-Postgres\
├── .env                      # Your credentials (create from .env.example)
├── docker-compose.yml        # Service definitions
├── Dockerfile.warehouse      # API container build
├── RUN.bat                   # Quick start script
│
├── warehouse\                # API code
│   ├── main.py              # FastAPI app
│   ├── engine.py            # DuckDB
│   ├── metadata.py          # MongoDB
│   └── sdk\client.py        # Python SDK
│
├── config\                   # Configuration
│   └── instacart_config.py
│
├── scripts\                  # Utility scripts
│   ├── download_kaggle_dataset.py
│   ├── upload_to_s3.py
│   └── register_metadata.py
│
└── terraform\                # AWS infrastructure
    ├── main.tf
    └── variables.tf
```

---

## ✅ Success Checklist

- [ ] Docker Desktop installed and running
- [ ] Python 3.9+ installed
- [ ] AWS CLI installed and configured
- [ ] `.env` file created with real credentials
- [ ] `docker-compose up -d` runs successfully
- [ ] Can access http://localhost:8000/docs
- [ ] Can see MongoDB in http://localhost:8081
- [ ] S3 bucket deployed via Terraform
- [ ] Data uploaded to S3
- [ ] Pipeline runs on Databricks
- [ ] Metadata registered to MongoDB
- [ ] Can query via API

---

## 💡 Tips

1. **Docker Desktop must be running** before any docker-compose commands
2. **Edit .env carefully** - no spaces around `=`
3. **Wait for services** - give them 30 seconds to start fully
4. **Check logs if issues** - `docker-compose logs -f`
5. **Use PowerShell** instead of CMD for better experience

---

**Easy deployment trên Windows!** 🚀
