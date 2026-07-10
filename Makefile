.PHONY: help install setup download upload bronze silver quality dbt-run dbt-test clean validate-env

# Default target
help:
	@echo "Instacart Lakehouse Pipeline - Makefile Commands"
	@echo ""
	@echo "Setup & Configuration:"
	@echo "  make install          - Install Python dependencies"
	@echo "  make setup            - Setup project (Terraform + Kaggle)"
	@echo "  make validate-env     - Validate environment variables"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make download         - Download Instacart dataset from Kaggle"
	@echo "  make upload           - Upload CSV files to GCS"
	@echo "  make bronze           - Run Bronze layer ingestion"
	@echo "  make silver           - Run Silver layer transformation"
	@echo "  make quality          - Run data quality checks"
	@echo "  make dbt-run          - Run dbt transformations"
	@echo "  make dbt-test         - Run dbt tests"
	@echo ""
	@echo "Full Pipeline:"
	@echo "  make pipeline         - Run full pipeline (bronze → silver → quality → dbt)"
	@echo ""
	@echo "Validation:"
	@echo "  make validate-bronze  - Validate Bronze layer tables"
	@echo "  make validate-silver  - Validate Silver layer tables"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean            - Clean build artifacts"
	@echo "  make format           - Format Python code with black"
	@echo "  make lint             - Lint Python code with flake8"
	@echo ""

# Environment variables
export PYTHONPATH := $(shell pwd):$(PYTHONPATH)
export SPARK_HOME := /opt/spark

# Install dependencies
install:
	@echo "📦 Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed"

# Setup project
setup: validate-env
	@echo "🔧 Setting up project infrastructure..."
	cd terraform && terraform init && terraform apply -auto-approve
	@echo "✅ Infrastructure provisioned"

# Validate environment
validate-env:
	@echo "🔍 Validating environment..."
	@test -n "$(GOOGLE_APPLICATION_CREDENTIALS)" || (echo "❌ GOOGLE_APPLICATION_CREDENTIALS not set" && exit 1)
	@test -f ~/.kaggle/kaggle.json || (echo "❌ Kaggle credentials not found" && exit 1)
	@echo "✅ Environment validated"

# Download dataset
download:
	@echo "📥 Downloading Instacart dataset from Kaggle..."
	python scripts/download_kaggle_dataset.py
	@echo "✅ Download complete"

# Upload to GCS
upload: validate-env
	@echo "📤 Uploading CSV files to GCS..."
	python scripts/upload_to_gcs.py
	@echo "✅ Upload complete"

# Bronze layer
bronze: validate-env
	@echo "🥉 Running Bronze layer ingestion..."
	spark-submit \
		--master local[*] \
		--driver-memory 4g \
		--executor-memory 4g \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,com.google.cloud.bigdataoss:gcs-connector:2.2.11 \
		pyspark/bronze_ingestion.py
	@echo "✅ Bronze layer complete"

# Silver layer
silver: validate-env
	@echo "🥈 Running Silver layer transformation..."
	spark-submit \
		--master local[*] \
		--driver-memory 4g \
		--executor-memory 4g \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,com.google.cloud.bigdataoss:gcs-connector:2.2.11 \
		pyspark/silver_transformation.py
	@echo "✅ Silver layer complete"

# Data quality checks
quality: validate-env
	@echo "🔍 Running data quality checks..."
	spark-submit \
		--master local[*] \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,com.google.cloud.bigdataoss:gcs-connector:2.2.11 \
		pyspark/data_quality_checks.py
	@echo "✅ Quality checks complete"

# dbt run
dbt-run:
	@echo "🏗️  Running dbt transformations..."
	cd dbt_instacart && dbt deps && dbt run --target prod
	@echo "✅ dbt run complete"

# dbt test
dbt-test:
	@echo "🧪 Running dbt tests..."
	cd dbt_instacart && dbt test --target prod
	@echo "✅ dbt tests complete"

# Full pipeline
pipeline: bronze silver quality dbt-run dbt-test
	@echo "✅ Full pipeline complete!"

# Validate Bronze layer
validate-bronze:
	@echo "🔍 Validating Bronze layer tables..."
	python scripts/validate_iceberg_tables.py --layer bronze

# Validate Silver layer
validate-silver:
	@echo "🔍 Validating Silver layer tables..."
	python scripts/validate_iceberg_tables.py --layer silver

# Format code
format:
	@echo "🎨 Formatting Python code..."
	black pyspark/ scripts/ config/
	@echo "✅ Code formatted"

# Lint code
lint:
	@echo "🔍 Linting Python code..."
	flake8 pyspark/ scripts/ config/ --max-line-length=100
	@echo "✅ Linting complete"

# Clean artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf spark-warehouse/ metastore_db/ derby.log
	cd dbt_instacart && rm -rf target/ dbt_packages/ logs/
	@echo "✅ Cleanup complete"

# Quick test (Bronze → Silver → Quality)
test: bronze silver quality
	@echo "✅ Quick test complete"
