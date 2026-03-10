#!/usr/bin/env python3
"""
Emergency cleanup script to remove stuck files from S3 ready folder.
Use this when Glue jobs report success but don't move files, blocking subsequent tests.

Usage:
  python cleanup_stuck_s3_files.py                    # List stuck files
  python cleanup_stuck_s3_files.py --delete           # Delete all files in ready folder
  python cleanup_stuck_s3_files.py --delete FILE.parquet  # Delete specific file
"""
import boto3
import sys
import argparse
from datetime import datetime

# Configuration
BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
S3_PREFIX = "bankfile/ready"

s3 = boto3.client("s3")

def list_ready_folder_files():
    """List all files currently in the ready folder."""
    print(f"📂 Checking s3://{BUCKET}/{S3_PREFIX}/")
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=S3_PREFIX + "/")
        contents = result.get("Contents", [])
        
        files = [obj for obj in contents if not obj["Key"].endswith("/")]
        
        if not files:
            print("✅ Ready folder is empty - no cleanup needed!")
            return []
        
        print(f"\n⚠️ Found {len(files)} file(s) in ready folder:\n")
        for obj in files:
            key = obj["Key"]
            size_mb = obj["Size"] / (1024 * 1024)
            last_modified = obj["LastModified"]
            age_minutes = (datetime.now(last_modified.tzinfo) - last_modified).total_seconds() / 60
            
            print(f"  📄 {key}")
            print(f"     Size: {size_mb:.2f} MB")
            print(f"     Last Modified: {last_modified} ({age_minutes:.1f} minutes ago)")
            print()
        
        return files
    
    except Exception as e:
        print(f"❌ Error listing ready folder: {e}")
        return []

def delete_file(file_key):
    """Delete a specific file from S3."""
    try:
        print(f"🗑️ Deleting s3://{BUCKET}/{file_key}")
        s3.delete_object(Bucket=BUCKET, Key=file_key)
        print(f"✅ Deleted: {file_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to delete {file_key}: {e}")
        return False

def delete_all_files(files):
    """Delete all files in ready folder."""
    if not files:
        print("ℹ️ No files to delete")
        return
    
    print(f"\n⚠️ About to delete {len(files)} file(s) from ready folder!")
    confirm = input("Type 'YES' to confirm deletion: ")
    
    if confirm != "YES":
        print("❌ Deletion cancelled")
        return
    
    deleted = 0
    failed = 0
    
    for obj in files:
        key = obj["Key"]
        if delete_file(key):
            deleted += 1
        else:
            failed += 1
    
    print(f"\n📊 Summary: {deleted} deleted, {failed} failed")

def main():
    parser = argparse.ArgumentParser(description="Cleanup stuck files in S3 ready folder")
    parser.add_argument("--delete", action="store_true", help="Delete files (otherwise just list)")
    parser.add_argument("filename", nargs="?", help="Specific filename to delete (optional)")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("S3 Ready Folder Cleanup Tool")
    print("=" * 80)
    print()
    
    files = list_ready_folder_files()
    
    if not args.delete:
        print("\nℹ️ Run with --delete to remove these files")
        return
    
    if args.filename:
        # Delete specific file
        matching_files = [obj for obj in files if args.filename in obj["Key"]]
        if not matching_files:
            print(f"❌ File not found: {args.filename}")
            return
        
        for obj in matching_files:
            delete_file(obj["Key"])
    else:
        # Delete all files
        delete_all_files(files)

if __name__ == "__main__":
    main()
