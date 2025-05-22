import os
import subprocess
from datetime import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time

# --------------------
# Configuration
# --------------------
BUCKET = "mtfpm-dev-s3-mtfpmstaging-us-east-1"
GLUE_JOB_NAME = "dm-bank-etl"
S3_PREFIX = "bankfile/2025/05"
ARCHIVE_PREFIX = "bankfile/archive/2025/05"
ERROR_CSV_PREFIX = "bankfile/error/"

s3 = boto3.client("s3")
glue = boto3.client("glue", region_name="us-east-1")

# --------------------
# Step 1: Generate test files
# --------------------
def run_generator_file(is_valid=True):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/{'valid' if is_valid else 'invalid'}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    output_filename = "DMBankData_valid" if is_valid else "DMBankData_invalid"

    cmd = [
        "python",
        "newaugsver.py",
        "--rows", "50",
        "--seed", "100",
        "--formats", "parquet",
        "--output-dir", output_dir,
        "--output", output_filename
    ]

    if not is_valid:
        cmd += ["--missing-data", "--missing-columns", "OrganizationTin", "ContactEmail"]

    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    full_path = os.path.join(output_dir, output_filename + ".parquet")
    return full_path

# --------------------
# Step 2: Upload to S3
# --------------------
def upload_to_s3(file_path):
    s3_key = f"{S3_PREFIX}/{os.path.basename(file_path)}"
    print(f"📤 Uploading {file_path} to s3://{BUCKET}/{s3_key}")
    s3.upload_file(file_path, BUCKET, s3_key)
    return s3_key

# --------------------
# Step 3: Trigger & monitor Glue
# --------------------
def wait_for_glue_success(job_name, timeout=600):
    print("🕒 Starting Glue job...")
    response = glue.start_job_run(JobName=job_name)
    run_id = response["JobRunId"]

    start_time = time.time()
    while time.time() - start_time < timeout:
        status = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
        print(f"⌛ Glue job status: {status}")
        if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
            return status == "SUCCEEDED"
        time.sleep(10)

    print("❌ Timeout waiting for Glue job to complete.")
    return False

# --------------------
# Step 4: Validate S3 Outputs
# --------------------
def check_s3_file_exists(prefix, keyword):
    print(f"🔍 Checking S3 prefix {prefix} for file containing '{keyword}'...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    for obj in result.get("Contents", []):
        if keyword in obj["Key"]:
            print(f"✅ Found: {obj['Key']}")
            return True
    print(f"❌ No file found in {prefix} containing '{keyword}'")
    return False

# --------------------
# Main Test Orchestration
# --------------------
def main():
    print(">>> Step 1: Generate test files")
    valid_file = run_generator_file(is_valid=True)
    invalid_file = run_generator_file(is_valid=False)

    print(">>> Step 2: Upload to S3")
    upload_to_s3(valid_file)
    upload_to_s3(invalid_file)

    print(">>> Step 3: Trigger and monitor Glue job")
    if not wait_for_glue_success(GLUE_JOB_NAME):
        print("❌ Glue job failed.")
        return

    time.sleep(20)  # buffer time for outputs to land

    print(">>> Step 4: Validate S3 outputs")
    assert check_s3_file_exists(ERROR_CSV_PREFIX, ".csv"), "❌ Error CSV not found"
    assert check_s3_file_exists(ARCHIVE_PREFIX, os.path.basename(valid_file)), "❌ Archive file not found"

    print("\n🎉 ALL VALIDATIONS PASSED ✅")

if __name__ == "__main__":
    main()
