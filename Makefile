# Instacart Data Lakehouse - Makefile
# Quick commands for common tasks

.PHONY: help install test clean docker-up docker-down docker-logs docker-rebuild

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Instacart Data Lakehouse - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# ============================================================================
# Local Development
# ============================================================================

install: ## Install Python dependencies
	@echo "$(BLUE)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	@echo "$(GREEN)✓ Installation complete$(NC)"

install-dev: ## Install dev dependencies
	@echo "$(BLUE)Installing dev dependencies...$(NC)"
	pip install -r requirements.txt
	pip install pytest black flake8 mypy
	@echo "$(GREEN)✓ Dev installation complete$(NC)"

setup-env: ## Copy .env.example to .env
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN)✓ Created .env file - Please edit with your credentials$(NC)"; \
	else \
		echo "$(YELLOW)⚠ .env already exists$(NC)"; \
	fi

# ============================================================================
# Docker Services
# ============================================================================

docker-up: ## Start all Docker services (MongoDB + API)
	@echo "$(BLUE)Starting Docker services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@echo ""
	@echo "Access URLs:"
	@echo "  API:           http://localhost:8000"
	@echo "  API Docs:      http://localhost:8000/docs"
	@echo "  Mongo Express: http://localhost:8081"

docker-down: ## Stop all Docker services
	@echo "$(BLUE)Stopping Docker services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ Services stopped$(NC)"

docker-restart: ## Restart all Docker services
	@echo "$(BLUE)Restarting Docker services...$(NC)"
	docker-compose restart
	@echo "$(GREEN)✓ Services restarted$(NC)"

docker-logs: ## Show logs from all services
	docker-compose logs -f

docker-logs-api: ## Show logs from Warehouse API only
	docker-compose logs -f warehouse-api

docker-logs-mongodb: ## Show logs from MongoDB only
	docker-compose logs -f mongodb

docker-ps: ## Show status of all services
	@docker-compose ps

docker-rebuild: ## Rebuild and restart API container
	@echo "$(BLUE)Rebuilding Warehouse API...$(NC)"
	docker-compose build warehouse-api
	docker-compose up -d warehouse-api
	@echo "$(GREEN)✓ API rebuilt and restarted$(NC)"

docker-clean: ## Stop and remove all containers, volumes, images
	@echo "$(RED)⚠ This will remove all data in MongoDB!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi local; \
		echo "$(GREEN)✓ Cleaned up$(NC)"; \
	fi

# ============================================================================
# MongoDB Management
# ============================================================================

mongo-shell: ## Open MongoDB shell
	@docker-compose exec mongodb mongosh -u admin -p admin123 instacart_metadata

mongo-backup: ## Backup MongoDB data
	@echo "$(BLUE)Backing up MongoDB...$(NC)"
	@mkdir -p ./backups
	docker-compose exec mongodb mongodump \
		-u admin -p admin123 \
		--authenticationDatabase admin \
		--db instacart_metadata \
		--out /tmp/backup
	docker cp instacart-mongodb:/tmp/backup ./backups/mongodb-$$(date +%Y%m%d_%H%M%S)
	@echo "$(GREEN)✓ Backup complete$(NC)"

mongo-restore: ## Restore MongoDB data (usage: make mongo-restore BACKUP=./backups/mongodb-20260710_120000)
	@if [ -z "$(BACKUP)" ]; then \
		echo "$(RED)Error: Please specify BACKUP path$(NC)"; \
		echo "Usage: make mongo-restore BACKUP=./backups/mongodb-20260710_120000"; \
		exit 1; \
	fi
	@echo "$(BLUE)Restoring MongoDB from $(BACKUP)...$(NC)"
	docker cp $(BACKUP) instacart-mongodb:/tmp/restore
	docker-compose exec mongodb mongorestore \
		-u admin -p admin123 \
		--authenticationDatabase admin \
		--db instacart_metadata \
		/tmp/restore/instacart_metadata
	@echo "$(GREEN)✓ Restore complete$(NC)"

# ============================================================================
# Data Pipeline
# ============================================================================

download-data: ## Download Instacart dataset from Kaggle
	@echo "$(BLUE)Downloading Instacart dataset...$(NC)"
	python scripts/download_kaggle_dataset.py
	@echo "$(GREEN)✓ Download complete$(NC)"

upload-s3: ## Upload data to S3
	@echo "$(BLUE)Uploading data to S3...$(NC)"
	python scripts/upload_to_s3.py
	@echo "$(GREEN)✓ Upload complete$(NC)"

register-metadata: ## Register Gold layer metadata to MongoDB
	@echo "$(BLUE)Registering metadata...$(NC)"
	python scripts/register_metadata.py
	@echo "$(GREEN)✓ Metadata registered$(NC)"

# ============================================================================
# Terraform
# ============================================================================

tf-init: ## Initialize Terraform
	@echo "$(BLUE)Initializing Terraform...$(NC)"
	cd terraform && terraform init
	@echo "$(GREEN)✓ Terraform initialized$(NC)"

tf-plan: ## Show Terraform plan
	@echo "$(BLUE)Planning Terraform changes...$(NC)"
	cd terraform && terraform plan

tf-apply: ## Apply Terraform changes
	@echo "$(BLUE)Applying Terraform changes...$(NC)"
	cd terraform && terraform apply
	@echo "$(GREEN)✓ Infrastructure deployed$(NC)"

tf-destroy: ## Destroy Terraform infrastructure
	@echo "$(RED)⚠ This will destroy all AWS resources!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		cd terraform && terraform destroy; \
		echo "$(GREEN)✓ Infrastructure destroyed$(NC)"; \
	fi

# ============================================================================
# dbt
# ============================================================================

dbt-debug: ## Test dbt connection
	@echo "$(BLUE)Testing dbt connection...$(NC)"
	cd dbt_instacart && dbt debug --profiles-dir ~/.dbt

dbt-compile: ## Compile dbt models
	@echo "$(BLUE)Compiling dbt models...$(NC)"
	cd dbt_instacart && dbt compile --profiles-dir ~/.dbt

dbt-run: ## Run dbt models
	@echo "$(BLUE)Running dbt models...$(NC)"
	cd dbt_instacart && dbt run --profiles-dir ~/.dbt --target prod
	@echo "$(GREEN)✓ dbt run complete$(NC)"

dbt-test: ## Run dbt tests
	@echo "$(BLUE)Running dbt tests...$(NC)"
	cd dbt_instacart && dbt test --profiles-dir ~/.dbt --target prod

dbt-docs: ## Generate and serve dbt documentation
	@echo "$(BLUE)Generating dbt docs...$(NC)"
	cd dbt_instacart && dbt docs generate --profiles-dir ~/.dbt
	cd dbt_instacart && dbt docs serve --port 8002

# ============================================================================
# Testing & Quality
# ============================================================================

test: ## Run Python tests
	@echo "$(BLUE)Running tests...$(NC)"
	pytest tests/ -v
	@echo "$(GREEN)✓ Tests complete$(NC)"

test-api: ## Test Warehouse API endpoints
	@echo "$(BLUE)Testing API...$(NC)"
	@curl -s http://localhost:8000/ | grep -q "service" && echo "$(GREEN)✓ Health check passed$(NC)" || echo "$(RED)✗ Health check failed$(NC)"
	@curl -s http://localhost:8000/datasets | grep -q "\[" && echo "$(GREEN)✓ List datasets passed$(NC)" || echo "$(RED)✗ List datasets failed$(NC)"

lint: ## Run code linting
	@echo "$(BLUE)Running linters...$(NC)"
	black --check .
	flake8 .
	@echo "$(GREEN)✓ Linting complete$(NC)"

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	black .
	@echo "$(GREEN)✓ Code formatted$(NC)"

# ============================================================================
# Utilities
# ============================================================================

clean: ## Clean temporary files
	@echo "$(BLUE)Cleaning temporary files...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache 2>/dev/null || true
	rm -rf dbt_instacart/target 2>/dev/null || true
	rm -rf dbt_instacart/dbt_packages 2>/dev/null || true
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

check-env: ## Verify environment setup
	@echo "$(BLUE)Checking environment...$(NC)"
	@echo ""
	@echo "Python version:"
	@python --version || echo "$(RED)✗ Python not found$(NC)"
	@echo ""
	@echo "Docker version:"
	@docker --version || echo "$(RED)✗ Docker not found$(NC)"
	@echo ""
	@echo "AWS CLI:"
	@aws --version || echo "$(RED)✗ AWS CLI not found$(NC)"
	@echo ""
	@echo "Terraform:"
	@terraform --version || echo "$(RED)✗ Terraform not found$(NC)"
	@echo ""
	@echo "dbt:"
	@dbt --version || echo "$(RED)✗ dbt not found$(NC)"
	@echo ""
	@if [ -f .env ]; then \
		echo "$(GREEN)✓ .env file exists$(NC)"; \
	else \
		echo "$(RED)✗ .env file missing$(NC)"; \
	fi

status: ## Show project status
	@echo "$(BLUE)Project Status$(NC)"
	@echo ""
	@echo "Docker Services:"
	@docker-compose ps 2>/dev/null || echo "  Not running"
	@echo ""
	@echo "S3 Bucket:"
	@aws s3 ls | grep instacart || echo "  Not found"
	@echo ""
	@echo "MongoDB:"
	@docker-compose exec mongodb mongosh --quiet --eval "db.datasets.countDocuments()" instacart_metadata 2>/dev/null || echo "  Not accessible"

# ============================================================================
# Quick Workflows
# ============================================================================

quick-start: setup-env docker-up ## Quick start (setup + start services)
	@echo ""
	@echo "$(GREEN)✓ Quick start complete!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit .env with your credentials"
	@echo "  2. Run: make tf-apply"
	@echo "  3. Run: make download-data"
	@echo "  4. Run: make upload-s3"
	@echo "  5. Run pipeline (spark-submit or Airflow DAG)"
	@echo "  6. Run: make register-metadata"

full-pipeline: download-data upload-s3 dbt-run register-metadata ## Run full local pipeline
	@echo "$(GREEN)✓ Full pipeline complete!$(NC)"

# ============================================================================
# Information
# ============================================================================

info: ## Show project information
	@echo "$(BLUE)Instacart Data Lakehouse$(NC)"
	@echo ""
	@echo "Architecture:"
	@echo "  CSV → S3 → PySpark (Bronze/Silver) → dbt (Gold) → MongoDB + DuckDB → FastAPI"
	@echo ""
	@echo "Stack:"
	@echo "  Storage:   AWS S3 (Iceberg)"
	@echo "  Compute:   Spark OSS (local dev / EC2 deploy)"
	@echo "  Transform: dbt-spark"
	@echo "  Metadata:  MongoDB (Docker)"
	@echo "  Query:     DuckDB (embedded)"
	@echo "  API:       FastAPI (Docker)"
	@echo ""
	@echo "Documentation:"
	@echo "  README.md              - Project overview"
	@echo "  TODO.md                - Deployment checklist"
	@echo "  QUICKSTART.md          - 30-minute guide"
	@echo "  DOCKER_DEPLOYMENT.md   - Docker guide"
	@echo ""
	@echo "Services:"
	@echo "  API:           http://localhost:8000"
	@echo "  API Docs:      http://localhost:8000/docs"
	@echo "  Mongo Express: http://localhost:8081"
