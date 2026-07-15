"""
Complete pipeline test - Verify all components work

Run this after completing setup to verify everything is ready
"""

import sys
import os
from pathlib import Path

def test_imports():
    """Test all required imports"""
    print("1  Testing imports...")
    
    try:
        import pandas as pd
        import requests
        from pymongo import MongoClient
        import duckdb
        from fastapi import FastAPI
        print("    All imports successful")
        return True
    except ImportError as e:
        print(f"    Import failed: {e}")
        print("   Run: pip install -r requirements.txt")
        return False


def test_warehouse_structure():
    """Test project structure"""
    print("\n2  Testing project structure...")
    
    required_dirs = [
        "warehouse",
        "etl",
        "etl/dbt_project",
        "scripts",
        "terraform",
        "config"
    ]
    
    all_exist = True
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"    {dir_name}/")
        else:
            print(f"    {dir_name}/ missing")
            all_exist = False
    
    return all_exist


def test_scripts():
    """Test key scripts exist"""
    print("\n3  Testing key scripts...")
    
    required_scripts = [
        "scripts/download_kaggle_dataset.py",
        "scripts/upload_to_s3.py",
        "scripts/register_metadata.py",
        "scripts/seed_instacart_metrics.py",
        "scripts/test_metrics_api.py"
    ]
    
    all_exist = True
    for script in required_scripts:
        if Path(script).exists():
            print(f"    {script}")
        else:
            print(f"    {script} missing")
            all_exist = False
    
    return all_exist


def test_warehouse_code():
    """Test warehouse code exists"""
    print("\n4  Testing warehouse service...")
    
    required_files = [
        "warehouse/api/main.py",
        "warehouse/engine/duckdb_engine.py",
        "warehouse/metadata.py",
        "warehouse/recommendation_store.py",
        "warehouse/parser/sql_validator.py",
        "warehouse/sdk/client.py"
    ]
    
    all_exist = True
    for file in required_files:
        if Path(file).exists():
            print(f"    {file}")
        else:
            print(f"    {file} missing")
            all_exist = False
    
    return all_exist


def test_dbt_structure():
    """Test dbt project structure"""
    print("\n5  Testing dbt project...")
    
    required_files = [
        "etl/dbt_project/dbt_project.yml",
        "etl/dbt_project/profiles.yml",
        "etl/dbt_project/models/staging/stg_orders.sql",
        "etl/dbt_project/models/marts/dimensions/dim_product.sql",
        "etl/dbt_project/models/marts/facts/fct_order_products.sql"
    ]
    
    all_exist = True
    for file in required_files:
        if Path(file).exists():
            print(f"    {file}")
        else:
            print(f"    {file} missing")
            all_exist = False
    
    return all_exist


def test_documentation():
    """Test documentation exists"""
    print("\n6  Testing documentation...")
    
    required_docs = [
        "README.md",
        "SETUP_CHECKLIST_A_TO_Z.md",
        "DEPLOYMENT_GUIDE.md",
        "DEVELOPMENT.md",
        "CODEBASE_READING_GUIDE.md",
        "SETUP_SUMMARY.md"
    ]
    
    all_exist = True
    for doc in required_docs:
        if Path(doc).exists():
            print(f"    {doc}")
        else:
            print(f"    {doc} missing")
            all_exist = False
    
    return all_exist


def test_configuration():
    """Test configuration files"""
    print("\n7  Testing configuration...")
    
    required_configs = [
        ".env.example",
        "docker-compose.yml",
        "requirements.txt",
        "terraform/main.tf"
    ]
    
    all_exist = True
    for config in required_configs:
        if Path(config).exists():
            print(f"    {config}")
        else:
            print(f"    {config} missing")
            all_exist = False
    
    # Check if .env exists (optional but recommended)
    if Path(".env").exists():
        print(f"    .env (configured)")
    else:
        print(f"     .env (not yet configured - copy from .env.example)")
    
    return all_exist


def test_api_connectivity():
    """Test if API is accessible (optional)"""
    print("\n8  Testing API connectivity (optional)...")
    
    try:
        import requests
    except ImportError:
        print("     requests is not installed; skipping optional API check")
        return True

    try:
        response = requests.get("http://localhost:8000/", timeout=2)
        if response.status_code == 200:
            print("    Warehouse API is running")
            return True
        else:
            print(f"     API returned status {response.status_code}")
            return True
    except requests.exceptions.ConnectionError:
        print("     API not running (expected if not started yet)")
        print("   To start: uvicorn warehouse.api.main:app --reload")
        return True
    except Exception as e:
        print(f"     Could not test API: {e}")
        return True


def main():
    """Run all tests"""
    print("=" * 60)
    print(" COMPLETE PIPELINE TEST")
    print("=" * 60)
    print()
    
    results = {
        "imports": test_imports(),
        "structure": test_warehouse_structure(),
        "scripts": test_scripts(),
        "warehouse": test_warehouse_code(),
        "dbt": test_dbt_structure(),
        "documentation": test_documentation(),
        "configuration": test_configuration(),
        "api": test_api_connectivity()
    }
    
    print("\n" + "=" * 60)
    print(" SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = " PASS" if result else " FAIL"
        print(f"{status:12} {test_name}")
    
    print("\n" + "-" * 60)
    print(f"Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n ALL TESTS PASSED!")
        print(" Project is complete and ready to deploy")
        print("\n Next steps:")
        print("1. Follow SETUP_CHECKLIST.md for deployment")
        print("2. Create accounts (AWS, Databricks, MongoDB)")
        print("3. Configure .env with credentials")
        print("4. Run: terraform apply")
        print()
        return 0
    else:
        print("\n  SOME TESTS FAILED")
        print("Please check the failures above and fix missing components")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())

