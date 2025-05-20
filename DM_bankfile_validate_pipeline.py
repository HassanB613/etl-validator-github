import subprocess
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
from datetime import datetime

# -------------------------
# Configuration
# -------------------------

# S3 paths
BUCKET = "mtfpm-dev-s3-mtfpmstaging-us-east-1"
S3_PREFIX = "bankfile/2025/05"
ARCHIVE_PREFIX = "bankfile/archive/2025/05"
ERROR_CSV_PREFIX = "bankfile/error/"

# Glue job name
GLUE_JOB_NAME = "dm-bank-etl"

# Output filenames
VALID_FILE = "DMBankData_valid.parquet"
INVALID_FILE = "DMBankData_invalid.parquet"

# AWS clients
s3 = boto3.client("s3")
glue = boto3.client("glue")
rds = boto3.client("rds-data")  # placeholder if using RDS Data API

# -------------------------
# Step 1: Generate Valid Parquet
# -------------------------
def generate_valid_parquet():
    print("🔧 Generating valid .parquet file...")
    subprocess.run(["python", "newaugsver.py", "--rows", "50", "--formats", "parquet", "--output", VALID_FILE], check=True)

# -------------------------
# Step 2: Inject Invalid Data
# -------------------------
def create_invalid_parquet():
    print("⚠️ Creating invalid .parquet file...")
    df = pd.read_parquet(VALID_FILE)
    df.loc[0, "ContactCode"] = "XX"  # invalid enum
    df.loc[1, "RoutingTransitNumber"] = "BADTYPE"  # invalid type
    df.loc[2, "OrganizationTin"] = None  # null where not allowed
    pq.write_table(pa.Table.from_pandas(df), INVALID_FILE)
    print("✅ Invalid file created")

# -------------------------
# Step 3: Upload to S3
# -------------------------
def upload_to_s3(filename):
    print(f"📤 Uploading {filename} to S3...")
    s3.upload_file(filename, BUCKET, f"{S3_PREFIX}/{filename}")

# -------------------------
# Step 4: Monitor Glue Job
# -------------------------
def wait_for_glue_success(job_name, timeout=600):
    print("🕒 Waiting for Glue job to complete...")
    start_time = time.time()
    job_run_id = glue.start_job_run(JobName=job_name)['JobRunId']

    while time.time() - start_time < timeout:
        run_state = glue.get_job_run(JobName=job_name, RunId=job_run_id)["JobRun"]["JobRunState"]
        if run_state in ("SUCCEEDED", "FAILED", "STOPPED"):
            print(f"✅ Glue job finished with status: {run_state}")
            return run_state == "SUCCEEDED"
        time.sleep(10)
    
    print("❌ Timeout waiting for Glue job")
    return False

# -------------------------
# Step 5: Validate Outputs
# -------------------------
def check_s3_exists(prefix, keyword):
    print(f"🔍 Checking S3 for {keyword} under {prefix}...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    for obj in result.get("Contents", []):
        if keyword in obj["Key"]:
            print(f"✅ Found: {obj['Key']}")
            return True
    print(f"❌ Not found: {keyword}")
    return False

# -------------------------
# Main
# -------------------------
def main():
    generate_valid_parquet()
    create_invalid_parquet()
    upload_to_s3(VALID_FILE)
    upload_to_s3(INVALID_FILE)

    if not wait_for_glue_success(GLUE_JOB_NAME):
        print("❌ Glue job failed.")
        return

    time.sleep(20)  # give downstream processes time to complete

    # Validate presence of outputs
    assert check_s3_exists(ERROR_CSV_PREFIX, ".csv"), "❌ Error CSV not found"
    assert check_s3_exists(ARCHIVE_PREFIX, VALID_FILE), "❌ Archive file not found"

    print("🎉 All validations passed.")

if __name__ == "__main__":
    main()
