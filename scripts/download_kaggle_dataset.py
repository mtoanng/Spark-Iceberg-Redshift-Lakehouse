"""
Download Instacart dataset from Kaggle
Requires Kaggle API credentials (~/.kaggle/kaggle.json)

Usage:
    python scripts/download_kaggle_dataset.py

Prerequisites:
    pip install kaggle
    kaggle competitions download -c instacart-market-basket-analysis
"""

import os
import sys
import zipfile
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "instacart"

def download_instacart_dataset():
    """Download Instacart dataset from Kaggle"""
    
    print("=" * 80)
    print("📥 DOWNLOADING INSTACART DATASET FROM KAGGLE")
    print("=" * 80)
    
    # Create data directory
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"📁 Target directory: {DATA_RAW_DIR}")
    
    # Initialize Kaggle API
    print("\n🔐 Authenticating with Kaggle API...")
    api = KaggleApi()
    api.authenticate()
    print("✅ Authentication successful")
    
    # Download competition files
    print("\n📥 Downloading competition files...")
    competition_name = "instacart-market-basket-analysis"
    
    try:
        api.competition_download_files(
            competition_name,
            path=str(DATA_RAW_DIR),
            quiet=False
        )
        print(f"✅ Download complete")
        
        # Extract ZIP files
        print("\n📦 Extracting files...")
        for zip_file in DATA_RAW_DIR.glob("*.zip"):
            print(f"  Extracting {zip_file.name}...")
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(DATA_RAW_DIR)
            zip_file.unlink()  # Remove zip after extraction
        
        print("✅ Extraction complete")
        
        # List downloaded files
        print("\n📋 Downloaded files:")
        for csv_file in sorted(DATA_RAW_DIR.glob("*.csv")):
            file_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            print(f"  ✓ {csv_file.name} ({file_size:.1f} MB)")
        
        print("\n" + "=" * 80)
        print("✅ DATASET DOWNLOAD COMPLETED SUCCESSFULLY")
        print("=" * 80)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error downloading dataset: {str(e)}")
        print("\n📋 Troubleshooting:")
        print("  1. Check ~/.kaggle/kaggle.json exists with your API credentials")
        print("  2. Accept competition rules at:")
        print("     https://www.kaggle.com/c/instacart-market-basket-analysis/rules")
        print("  3. Run: kaggle competitions download -c instacart-market-basket-analysis")
        return 1


if __name__ == "__main__":
    sys.exit(download_instacart_dataset())
