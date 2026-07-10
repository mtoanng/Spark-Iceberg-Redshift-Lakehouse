"""
Upload Instacart dataset from local to AWS S3 raw layer

Usage:
    python scripts/upload_to_s3.py

Prerequisites:
    pip install boto3
    AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
"""

import sys
import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "instacart"

sys.path.insert(0, str(PROJECT_ROOT))
from config.instacart_config import (
    S3_BUCKET, S3_RAW_PREFIX, AWS_REGION, INSTACART_FILES
)


def upload_to_s3():
    """Upload all Instacart CSV files to S3"""
    
    print("=" * 80)
    print("📤 UPLOADING INSTACART DATA TO S3")
    print("=" * 80)
    print(f"🪣 Bucket: {S3_BUCKET}")
    print(f"🌎 Region: {AWS_REGION}")
    print(f"📂 Prefix: {S3_RAW_PREFIX}")
    print(f"📁 Local Dir: {DATA_RAW_DIR}")
    print("=" * 80 + "\n")
    
    # Check AWS credentials
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        print("❌ ERROR: AWS credentials not found!")
        print("\nSet environment variables:")
        print("  export AWS_ACCESS_KEY_ID=your_key_id")
        print("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
        return 1
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        # Test connection
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"✅ Connected to S3 bucket: {S3_BUCKET}\n")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ Bucket '{S3_BUCKET}' does not exist!")
            print("Run terraform apply to create bucket first.")
        else:
            print(f"❌ Error connecting to S3: {str(e)}")
        return 1
    
    # Upload each file
    uploaded = 0
    failed = 0
    
    for file_key, filename in INSTACART_FILES.items():
        local_path = DATA_RAW_DIR / filename
        s3_key = f"{S3_RAW_PREFIX}/{filename}"
        
        if not local_path.exists():
            print(f"⚠️  Skipping {filename} (not found locally)")
            failed += 1
            continue
        
        file_size = local_path.stat().st_size / (1024 * 1024)  # MB
        print(f"📤 Uploading {filename} ({file_size:.1f} MB)...")
        
        try:
            s3_client.upload_file(
                str(local_path),
                S3_BUCKET,
                s3_key,
                Callback=lambda bytes_transferred: None  # Can add progress bar here
            )
            print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}\n")
            uploaded += 1
        except Exception as e:
            print(f"❌ Error uploading {filename}: {str(e)}\n")
            failed += 1
    
    # Summary
    print("=" * 80)
    print("📊 UPLOAD SUMMARY")
    print("=" * 80)
    print(f"✅ Uploaded: {uploaded} files")
    print(f"❌ Failed: {failed} files")
    print(f"📍 S3 Location: s3://{S3_BUCKET}/{S3_RAW_PREFIX}/")
    print("=" * 80)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(upload_to_s3())
