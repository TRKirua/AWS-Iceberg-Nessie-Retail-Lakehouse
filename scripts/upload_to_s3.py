#!/usr/bin/env python3
"""
Upload data files to AWS S3.

This script uploads local CSV files to S3 for processing by the lakehouse pipeline.

Usage:
    # Upload batch files (default for this project)
    python scripts/upload_to_s3.py --batch-files

    # Upload everything
    python scripts/upload_to_s3.py --all

    # List files currently in S3
    python scripts/upload_to_s3.py --list
"""

import os
import sys
import argparse
from pathlib import Path
from glob import glob
import boto3
from dotenv import load_dotenv

# Add src to path for imports
project_root = Path(__file__).resolve().parents[1]
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from lakehouse.settings import AWS_BUCKET, AWS_REGION

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def create_s3_session():
    """Create and return an S3 client session."""
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    return session.client("s3")


def upload_file(s3_client, local_path: str, s3_key: str) -> bool:
    """
    Upload a single file to S3.

    Args:
        s3_client: Boto3 S3 client
        local_path: Path to local file
        s3_key: S3 key (destination path)

    Returns:
        True if successful, False otherwise
    """
    local_file = Path(local_path)

    if not local_file.exists():
        print(f"File not found: {local_path}")
        return False

    try:
        print(f"{local_file.name} -> s3://{AWS_BUCKET}/{s3_key}")
        s3_client.upload_file(str(local_file), AWS_BUCKET, s3_key)
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False


def upload_source_files(s3_client, upload_main: bool = True, upload_batches: bool = False):
    """
    Upload source CSV files to S3.

    Args:
        s3_client: Boto3 S3 client
        upload_main: Upload main source file
        upload_batches: Upload batch files
    """
    project_root = Path(__file__).resolve().parents[1]
    uploads = []

    if upload_main:
        main_file = project_root / "data" / "raw" / "superstore_sales.csv"
        if main_file.exists():
            uploads.append((str(main_file), "raw/sales/superstore_sales.csv"))

    if upload_batches:
        batch_dir = project_root / "data" / "raw_batches"
        if batch_dir.exists():
            # Find all batch CSV files
            batch_files = sorted(batch_dir.glob("sales_batch_*.csv"))
            for batch_file in batch_files:
                uploads.append((str(batch_file), f"raw/batches/{batch_file.name}"))

    if not uploads:
        print("No files found to upload.")
        return

    print(f"\nUploading {len(uploads)} file(s) to S3 bucket: {AWS_BUCKET}")
    print("-" * 60)

    success_count = 0
    for local_path, s3_key in uploads:
        if upload_file(s3_client, local_path, s3_key):
            success_count += 1

    print("-" * 60)
    print(f"Upload complete: {success_count}/{len(uploads)} files uploaded")


def list_s3_files(s3_client, prefix: str = ""):
    """
    List files in S3 bucket with given prefix.

    Args:
        s3_client: Boto3 S3 client
        prefix: S3 prefix to filter
    """
    print(f"Files in s3://{AWS_BUCKET}/{prefix or '*'}:")
    print("-" * 60)

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=AWS_BUCKET, Prefix=prefix)

    found_files = False
    for page in pages:
        if "Contents" in page:
            found_files = True
            for obj in page["Contents"]:
                size_kb = obj["Size"] / 1024
                print(f"  📄 {obj['Key']} ({size_kb:.1f} KB)")

    if not found_files:
        print("  No files found.")

    print("-" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Upload data files to AWS S3"
    )
    parser.add_argument(
        "--batch-files",
        action="store_true",
        help="Upload batch files from data/raw_batches/"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Upload all files (main + batches)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List files currently in S3"
    )
    args = parser.parse_args()

    # Create S3 session
    s3 = create_s3_session()

    # List files if requested
    if args.list:
        list_s3_files(s3)
        return 0

    # Determine what to upload
    upload_main = args.all
    upload_batches = args.batch_files or args.all

    if not upload_main and not upload_batches:
        print("No files specified for upload.")
        print("Use --batch-files to upload batch files or --all to upload everything.")
        print("Use --list to see files currently in S3.")
        parser.print_help()
        return 1

    # Upload files
    upload_source_files(s3, upload_main, upload_batches)

    return 0


if __name__ == "__main__":
    exit(main())
