# Docker Deployment Guide

**Deploy MongoDB + Warehouse API với Docker Compose**

---

## 🎯 Overview

Deploy tất cả services cùng lúc:
- **MongoDB** (metadata catalog)
- **Warehouse API** (FastAPI + DuckDB)
- **MongoDB Express** (Web UI - optional)

**Total deployment time: 5 minutes** ⚡

---

## 📋 Prerequisites

- Docker Desktop installed
- AWS credentials configured
- Gold layer data đã có trên S3

---

## 🚀 Quick Start

### 1. Setup Environment (1 minute)

```bash
# Copy và edit .env
cp .env.example .env

# Edit .env với credentials của bạn:
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY  
# - S3_BUCKET
```

### 2. Start All Services (2 minutes)

```bash
# Build và start tất cả services
docker-compose up -d

# Check logs
docker-compose logs -f

# Wait for all services to be healthy (~30 seconds)
```

### 3. Verify Services (1 minute)

```bash
# Check status
docker-compose ps

# Should see:
# ✓ instacart-mongodb        (healthy)
# ✓ instacart-warehouse-api  (healthy)
# ✓ instacart-mongo-express  (healthy)
```

### 4. Test API (1 minute)

```bash
# Health check
curl http://localhost:8000/

# List datasets
curl http://localhost:8000/datasets

# Test query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM gold.fct_order_products"}'
```

---

## 🌐 Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Warehouse API** | http://localhost:8000 | None |
| **API Docs** | http://localhost:8000/docs | None |
| **MongoDB** | localhost:27017 | admin / admin123 |
| **Mongo Express** | http://localhost:8081 | admin / admin |

---

## 📊 Architecture

```
┌─────────────────────────────────────┐
│         Docker Compose              │
│                                     │
│  ┌──────────────────────────────┐  │
│  │ MongoDB Container            │  │
│  │ - Port: 27017                │  │
│  │ - Volume: mongodb-data       │  │
│  │ - Init: mongo-init/init-db.js│  │
│  └──────────────────────────────┘  │
│             ↓                       │
│  ┌──────────────────────────────┐  │
│  │ Warehouse API Container      │  │
│  │ - FastAPI + DuckDB           │  │
│  │ - Port: 8000                 │  │
│  │ - Mounts: ~/.aws credentials │  │
│  └──────────────────────────────┘  │
│             ↓                       │
│  ┌──────────────────────────────┐  │
│  │ Mongo Express (Optional)     │  │
│  │ - Web UI for MongoDB         │  │
│  │ - Port: 8081                 │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
          ↓ Queries S3 Gold Layer
    s3://bucket/gold/
```

---

## 🔧 Docker Compose Services

### MongoDB Service

```yaml
mongodb:
  image: mongo:7.0
  ports: ["27017:27017"]
  environment:
    MONGO_INITDB_ROOT_USERNAME: admin
    MONGO_INITDB_ROOT_PASSWORD: admin123
  volumes:
    - mongodb-data:/data/db
    - ./mongo-init:/docker-entrypoint-initdb.d
```

**Features:**
- Auto-initialization với `mongo-init/init-db.js`
- Persistent data với Docker volume
- Health checks enabled

### Warehouse API Service

```yaml
warehouse-api:
  build: 
    context: .
    dockerfile: Dockerfile.warehouse
  ports: ["8000:8000"]
  environment:
    MONGODB_URI: mongodb://admin:admin123@mongodb:27017/
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
  volumes:
    - ./warehouse:/app/warehouse  # Live reload
    - ~/.aws:/root/.aws:ro        # AWS credentials
```

**Features:**
- FastAPI với auto-reload (development)
- DuckDB embedded (no separate container)
- Reads S3 directly từ Gold layer
- Mounts AWS credentials từ host

---

## 📝 Common Commands

### Start Services

```bash
# Start all (detached mode)
docker-compose up -d

# Start with logs
docker-compose up

# Start specific service
docker-compose up -d mongodb
docker-compose up -d warehouse-api
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f warehouse-api
docker-compose logs -f mongodb

# Last 100 lines
docker-compose logs --tail=100 warehouse-api
```

### Stop/Restart

```bash
# Stop all
docker-compose stop

# Restart all
docker-compose restart

# Restart specific service
docker-compose restart warehouse-api

# Stop and remove containers
docker-compose down
```

### Rebuild

```bash
# Rebuild API after code changes
docker-compose build warehouse-api

# Rebuild and restart
docker-compose up -d --build warehouse-api
```

---

## 🔄 Register Metadata to MongoDB

After dbt completes, register Gold tables to MongoDB:

```bash
# Option 1: Run from host
python scripts/register_metadata.py

# Option 2: Run from API container
docker-compose exec warehouse-api python /app/scripts/register_metadata.py
```

---

## 🧪 Testing

### Test MongoDB Connection

```bash
# From host
docker-compose exec mongodb mongosh \
  -u admin -p admin123 \
  --eval "db.adminCommand('ping')"

# Should return: { ok: 1 }
```

### Test API Endpoints

```bash
# Health check
curl http://localhost:8000/

# List datasets
curl http://localhost:8000/datasets | jq

# Get dataset metadata
curl http://localhost:8000/datasets/gold.dim_product | jq

# Execute query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM gold.dim_product LIMIT 5"}' | jq
```

### Test Python SDK

```python
from warehouse.sdk import WarehouseClient

client = WarehouseClient("http://localhost:8000")

# List datasets
datasets = client.list_datasets()
print(f"Found {len(datasets)} datasets")

# Query
df = client.query("SELECT * FROM gold.dim_product LIMIT 10")
print(df)
```

---

## 🗄️ MongoDB Management

### Access MongoDB Shell

```bash
# Connect to MongoDB
docker-compose exec mongodb mongosh \
  -u admin -p admin123 \
  instacart_metadata

# MongoDB shell commands:
> show collections
> db.datasets.find().pretty()
> db.datasets.countDocuments()
> exit
```

### Use Mongo Express (Web UI)

1. Open browser: http://localhost:8081
2. Login: admin / admin
3. Navigate: `instacart_metadata` → `datasets`
4. View/edit documents

### Backup MongoDB Data

```bash
# Backup to file
docker-compose exec mongodb mongodump \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  --db instacart_metadata \
  --out /tmp/backup

# Copy backup to host
docker cp instacart-mongodb:/tmp/backup ./mongodb-backup
```

### Restore MongoDB Data

```bash
# Copy backup to container
docker cp ./mongodb-backup instacart-mongodb:/tmp/backup

# Restore
docker-compose exec mongodb mongorestore \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  --db instacart_metadata \
  /tmp/backup/instacart_metadata
```

---

## 🛠️ Troubleshooting

### Issue: MongoDB won't start

```bash
# Check logs
docker-compose logs mongodb

# Remove volume and restart
docker-compose down -v
docker-compose up -d mongodb
```

### Issue: API can't connect to MongoDB

```bash
# Check MongoDB is healthy
docker-compose ps

# Test connection from API container
docker-compose exec warehouse-api \
  python -c "from pymongo import MongoClient; print(MongoClient('mongodb://admin:admin123@mongodb:27017/').server_info())"
```

### Issue: API can't access S3

```bash
# Check AWS credentials are mounted
docker-compose exec warehouse-api ls -la /root/.aws

# Test S3 access
docker-compose exec warehouse-api \
  python -c "import boto3; print(boto3.client('s3').list_buckets())"
```

### Issue: DuckDB can't read Iceberg

```bash
# Check S3 path is correct
docker-compose exec warehouse-api \
  env | grep S3_GOLD_PATH

# Test DuckDB
docker-compose exec warehouse-api python -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL iceberg')
conn.execute('LOAD iceberg')
print('DuckDB Iceberg extension loaded')
"
```

---

## 🔐 Security Notes

### Development Setup (Current)
- MongoDB: No TLS, admin/admin123
- API: No authentication
- Mongo Express: Basic auth (admin/admin)

### Production Recommendations
1. **MongoDB:**
   - Use strong passwords
   - Enable TLS
   - Limit network access
   - Use MongoDB Atlas instead

2. **API:**
   - Add API key authentication
   - Enable HTTPS
   - Rate limiting
   - CORS configuration

3. **Secrets:**
   - Use Docker secrets
   - Don't commit `.env` to git
   - Rotate credentials regularly

---

## 📊 Resource Usage

| Service | Memory | CPU | Disk |
|---------|--------|-----|------|
| MongoDB | ~200MB | 1-2% | ~100MB |
| Warehouse API | ~150MB | 1-2% | Minimal |
| Mongo Express | ~50MB | <1% | Minimal |
| **Total** | **~400MB** | **~5%** | **~100MB** |

---

## 🎯 Production Deployment

For production, consider:

### Option 1: Docker Swarm
```bash
docker swarm init
docker stack deploy -c docker-compose.yml instacart
```

### Option 2: Kubernetes
```bash
# Convert docker-compose to k8s
kompose convert
kubectl apply -f .
```

### Option 3: Managed Services
- **MongoDB:** MongoDB Atlas (recommended)
- **API:** AWS ECS / Google Cloud Run
- **Storage:** Keep S3

---

## 📝 Development Workflow

```bash
# 1. Make code changes in warehouse/

# 2. Restart API to apply changes
docker-compose restart warehouse-api

# 3. Test changes
curl http://localhost:8000/docs

# 4. Check logs for errors
docker-compose logs -f warehouse-api

# 5. If everything works, commit code
git add warehouse/
git commit -m "feat: add new endpoint"
```

---

## ✅ Checklist

Before deploying:
- [ ] `.env` file configured with AWS credentials
- [ ] S3 bucket exists with Gold layer data
- [ ] Docker Desktop running
- [ ] Ports 8000, 8081, 27017 are free

After deploying:
- [ ] All 3 services are healthy
- [ ] API responds at http://localhost:8000
- [ ] MongoDB has metadata registered
- [ ] Can query Gold tables via API

---

**Simple, fast, và easy to manage!** 🚀
