"""
Setup Kaggle API credentials directory
Creates ~/.kaggle/ directory and provides instructions

Usage:
    python scripts/setup_kaggle.py
"""

import os
import sys
from pathlib import Path

def setup_kaggle_dir():
    """Create .kaggle directory and guide user"""
    
    print("=" * 80)
    print("🔐 KAGGLE API SETUP")
    print("=" * 80)
    
    # Get user home directory
    home_dir = Path.home()
    kaggle_dir = home_dir / ".kaggle"
    kaggle_json = kaggle_dir / "kaggle.json"
    
    print(f"\n📁 Kaggle directory: {kaggle_dir}")
    print(f"📄 Credentials file: {kaggle_json}")
    
    # Create .kaggle directory
    if not kaggle_dir.exists():
        print(f"\n📁 Creating directory: {kaggle_dir}")
        kaggle_dir.mkdir(parents=True, exist_ok=True)
        print("✅ Directory created")
    else:
        print(f"\n✅ Directory already exists: {kaggle_dir}")
    
    # Check if kaggle.json exists
    if kaggle_json.exists():
        print(f"\n✅ Credentials file already exists: {kaggle_json}")
        print("\n🔍 Testing Kaggle API...")
        
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
            api.authenticate()
            print("✅ Kaggle API authentication successful!")
            
            # Test listing competitions
            print("\n🔍 Testing API access...")
            import subprocess
            result = subprocess.run(
                ["kaggle", "competitions", "list", "-s", "instacart"],
                capture_output=True,
                text=True
            )
            
            if "instacart-market-basket-analysis" in result.stdout:
                print("✅ Can access Instacart competition")
            else:
                print("⚠️  Cannot find Instacart competition in listing")
                print("   Make sure to accept competition rules at:")
                print("   https://www.kaggle.com/c/instacart-market-basket-analysis/rules")
            
            return 0
            
        except Exception as e:
            print(f"❌ Error testing API: {str(e)}")
            print("\n📋 Please check your kaggle.json file is valid")
            return 1
    
    else:
        print(f"\n⚠️  Credentials file NOT found: {kaggle_json}")
        print("\n" + "=" * 80)
        print("📋 SETUP INSTRUCTIONS")
        print("=" * 80)
        print("\nStep 1: Get your Kaggle API credentials")
        print("  1. Go to: https://www.kaggle.com/settings/account")
        print("  2. Scroll to 'API' section")
        print("  3. Click 'Create New Token'")
        print("  4. This downloads 'kaggle.json' to your Downloads folder")
        
        print("\nStep 2: Move kaggle.json to the correct location")
        print(f"  Windows PowerShell:")
        print(f'    Move-Item "$env:USERPROFILE\\Downloads\\kaggle.json" "{kaggle_json}"')
        print(f"\n  Or manually:")
        print(f'    Copy from: C:\\Users\\{os.getenv("USERNAME")}\\Downloads\\kaggle.json')
        print(f"    To: {kaggle_json}")
        
        print("\nStep 3: Accept competition rules")
        print("  1. Visit: https://www.kaggle.com/c/instacart-market-basket-analysis/rules")
        print("  2. Click 'I Understand and Accept'")
        
        print("\nStep 4: Run this script again to verify")
        print("  python scripts/setup_kaggle.py")
        
        print("\n" + "=" * 80)
        
        # Offer to open browser
        try:
            import webbrowser
            print("\n💡 Would you like to open Kaggle settings in browser? (y/n): ", end="")
            response = input().strip().lower()
            if response == 'y':
                webbrowser.open("https://www.kaggle.com/settings/account")
                print("✅ Opened browser")
        except:
            pass
        
        return 1


if __name__ == "__main__":
    sys.exit(setup_kaggle_dir())
