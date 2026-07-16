# 🚀 QUICK START - Fix Python 3.13 Issue

**Problem:** `dbt-utils` không hỗ trợ Python 3.13

**Solution:** Cài Python 3.12.7 bằng PowerShell script tự động

---

## ⚡ OPTION 1: Automated Script (Recommended)

### Run 1 lệnh để setup tất cả:

```powershell
# Mở PowerShell as Administrator
# Navigate to project folder
cd C:\Users\ADMIN\BATCHING\Spark-Iceberg-DuckDB-Lakehouse

# Run setup script
powershell -ExecutionPolicy Bypass -File .\setup_python_312.ps1
```

### Script sẽ tự động:
1. ⬇️ Download Python 3.12.7 (25MB)
2. 📦 Install Python 3.12.7 vào `C:\Python312`
3. 🗑️ Remove old `.venv` (Python 3.13)
4. 🆕 Create new `.venv` with Python 3.12
5. 📚 Install tất cả packages từ `requirements.txt`

**⏱️ Thời gian:** 10-15 phút (tùy tốc độ internet)

**✅ Sau khi hoàn thành:**
```powershell
# Activate venv
.\.venv\Scripts\activate

# Verify
python --version
# Expected: Python 3.12.7

pip list | findstr dbt-utils
# Expected: dbt-utils 1.x.x
```

---

## 🔧 OPTION 2: Manual Steps

Nếu script không chạy, làm thủ công:

### Step 1: Download Python 3.12.7
```powershell
# Download từ:
https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe

# Hoặc dùng PowerShell:
Invoke-WebRequest -Uri "https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe" -OutFile "$env:TEMP\python-3.12.7-amd64.exe"
```

### Step 2: Install Python 3.12.7
```powershell
# Run installer
Start-Process -FilePath "$env:TEMP\python-3.12.7-amd64.exe" -ArgumentList "/quiet", "InstallAllUsers=1", "PrependPath=1", "TargetDir=C:\Python312" -Wait
```

### Step 3: Remove old venv
```powershell
# Deactivate current venv (if activated)
deactivate

# Remove old .venv
Remove-Item -Path .venv -Recurse -Force
```

### Step 4: Create new venv with Python 3.12
```powershell
C:\Python312\python.exe -m venv .venv
```

### Step 5: Install packages
```powershell
# Activate new venv
.\.venv\Scripts\activate

# Upgrade pip
python.exe -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

**⏱️ Thời gian:** 15-20 phút

---

## ❓ TROUBLESHOOTING

### Error: "ExecutionPolicy restricted"
```powershell
# Run PowerShell as Administrator
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Error: "Cannot remove .venv - file in use"
```powershell
# Close VSCode/IDEs
# Close all terminals
# Try again
```

### Error: "Download failed - SSL error"
```powershell
# Use browser to download manually:
# https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe
# Then run installer manually
```

### Error: "pip install fails for specific package"
```powershell
# Install packages one by one to find culprit
pip install boto3
pip install dbt-core
pip install dbt-spark
# etc...
```

---

## ✅ VERIFICATION CHECKLIST

After installation, verify:

```powershell
# 1. Check Python version
python --version
# Expected: Python 3.12.7

# 2. Check critical packages
pip list | findstr "dbt-utils boto3 pyspark duckdb fastapi"
# Expected: All packages listed

# 3. Test imports
python -c "import boto3, dbt, fastapi, duckdb, pyspark, pymongo, sqlglot; print('✓ All packages OK')"
# Expected: ✓ All packages OK

# 4. Check dbt
cd etl\dbt_project
dbt --version
# Expected: dbt version 1.x.x

# 5. Check requirements
pip check
# Expected: No broken requirements found
```

---

## 🎯 NEXT STEPS

After Python 3.12 setup thành công:

1. ✅ Continue với **SETUP_CHECKLIST_A_TO_Z.md Phase 4** (Dataset Download)
2. ✅ Hoặc verify Terraform: `terraform validate`
3. ✅ Hoặc test local pipeline: `python test_complete_pipeline.py`

---

## 📊 WHY PYTHON 3.12?

| Python Version | dbt-utils Support | Status |
|----------------|-------------------|--------|
| 3.13.x         | ❌ NOT supported  | **KHÔNG dùng được** |
| 3.12.x         | ✅ Supported      | **RECOMMEND** |
| 3.11.x         | ✅ Supported      | OK |
| 3.10.x         | ✅ Supported      | OK |
| 3.9.x          | ✅ Supported      | OK (old) |

**Lý do:** `dbt-utils` version 1.x.x requires Python >=3.9,<3.13 (not 3.13!)

---

## 💡 TIPS

1. **Không uninstall Python 3.13** - có thể dùng cho projects khác
2. **Dùng Python 3.12** cho project này specifically
3. **Activate venv** mỗi khi mở terminal mới: `.\.venv\Scripts\activate`
4. **Check Python version** trong venv: `python --version` (phải là 3.12.x)

---

**🎊 Good luck với setup! 🚀**
