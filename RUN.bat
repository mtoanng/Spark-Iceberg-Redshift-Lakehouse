@echo off
REM Quick deployment script for Windows

echo =====================================
echo Instacart Data Lakehouse Deployment
echo =====================================
echo.

REM Check if .env exists
if not exist .env (
    echo [SETUP] Creating .env file...
    copy .env.example .env
    echo [INFO] Please edit .env with your AWS credentials
    echo [INFO] Then run this script again
    pause
    exit /b
)

echo [1/4] Checking Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker not found. Please install Docker Desktop
    pause
    exit /b 1
)
echo [OK] Docker is installed

echo.
echo [2/4] Building and starting services...
docker-compose up -d

echo.
echo [3/4] Waiting for services to be healthy (30 seconds)...
timeout /t 30 /nobreak >nul

echo.
echo [4/4] Checking service status...
docker-compose ps

echo.
echo =====================================
echo Deployment Complete!
echo =====================================
echo.
echo Services Running:
echo   - MongoDB:       localhost:27017
echo   - Warehouse API: http://localhost:8000
echo   - API Docs:      http://localhost:8000/docs
echo   - Mongo Express: http://localhost:8081
echo.
echo Next Steps:
echo   1. Deploy AWS infrastructure: cd terraform ^&^& terraform apply
echo   2. Download data: python scripts\download_kaggle_dataset.py
echo   3. Upload to S3: python scripts\upload_to_s3.py
echo   4. Run pipeline on Databricks
echo   5. Register metadata: python scripts\register_metadata.py
echo.
pause
