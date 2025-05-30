import os
import subprocess
from datetime import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time
import requests  # Add this for TestRail integration

# --------------------
# Configuration
# --------------------
BUCKET = "mtfpm-dev-s3-mtfdmstaging-us-east-1"  # Update to the correct bucket name
GLUE_JOB_NAME = "mtfpm-bankfile-validation-error-handling-dev"
S3_PREFIX = "bankfile/ready"           # Update to the correct prefix
ARCHIVE_PREFIX = "bankfile/archive/2025/05"
ERROR_CSV_PREFIX = "bankfile/error/"

s3 = boto3.client("s3")
glue = boto3.client("glue", region_name="us-east-1")

# --------------------
# TestRail Configuration
# --------------------
TESTRAIL_URL = "https://testrailent.cms.gov/"
TESTRAIL_USERNAME = "BOSF"
TESTRAIL_API_KEY = "B81s5sde"
TESTRAIL_RUN_ID = 38269  # Active TestRail run ID
TESTRAIL_TEST_ID = 17539214  # Replace with your TestRail test ID

def report_to_testrail(test_id, status, comment):
    """
    Report test results to TestRail.
    :param test_id: TestRail test ID (not test case ID)
    :param status: Test result status (1=Passed, 2=Blocked, 3=Untested, 4=Retest, 5=Failed)
    :param comment: Additional comments for the test result
    """
    url = f"{TESTRAIL_URL}index.php?/api/v2/add_result/{test_id}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "status_id": status,
        "comment": comment
    }
    response = requests.post(url, json=payload, auth=(TESTRAIL_USERNAME, TESTRAIL_API_KEY))
    if response.status_code == 200:
        print(f"✅ Test result reported to TestRail for test {test_id}")
    else:
        print(f"❌ Failed to report test result to TestRail: {response.text}")

def add_case_to_run(run_id, case_id):
    """
    Add a test case to an existing TestRail run.
    :param run_id: TestRail run ID
    :param case_id: TestRail case ID
    """
    print(f"🔄 Ensuring test case {case_id} is part of run {run_id}...")
    # Get the current list of test case IDs in the run
    url = f"{TESTRAIL_URL}index.php?/api/v2/get_tests/{run_id}"
    response = requests.get(url, auth=(TESTRAIL_USERNAME, TESTRAIL_API_KEY))
    if response.status_code != 200:
        print(f"❌ Failed to fetch test cases in run {run_id}: {response.text}")
        return False

    # Check if the case is already in the run
    tests = response.json()
    for test in tests:
        if test["case_id"] == case_id:
            print(f"✅ Test case {case_id} is already part of run {run_id}")
            return True

    # Add the test case to the run
    url = f"{TESTRAIL_URL}index.php?/api/v2/update_run/{run_id}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "case_ids": [test["case_id"] for test in tests] + [case_id]  # Add the new case ID
    }
    response = requests.post(url, json=payload, auth=(TESTRAIL_USERNAME, TESTRAIL_API_KEY))
    if response.status_code == 200:
        print(f"✅ Test case {case_id} successfully added to run {run_id}")
        return True
    else:
        print(f"❌ Failed to add test case {case_id} to run {run_id}: {response.text}")
        return False

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
    glue_job_success = wait_for_glue_success(GLUE_JOB_NAME)
    if not glue_job_success:
        print("❌ Glue job failed.")
        report_to_testrail(TESTRAIL_TEST_ID, 5, "Glue job failed.")
        return

    time.sleep(20)  # buffer time for outputs to land

    print(">>> Step 4: Validate S3 outputs")
    error_csv_found = check_s3_file_exists(ERROR_CSV_PREFIX, ".csv")
    archive_file_found = check_s3_file_exists(ARCHIVE_PREFIX, os.path.basename(valid_file))

    if error_csv_found and archive_file_found:
        print("\n🎉 ALL VALIDATIONS PASSED ✅")
        report_to_testrail(TESTRAIL_TEST_ID, 1, "All validations passed.")
    else:
        print("\n❌ VALIDATIONS FAILED")
        failure_reason = (
            "Error CSV not found" if not error_csv_found else "Archive file not found"
        )
        report_to_testrail(TESTRAIL_TEST_ID, 5, f"Validation failed: {failure_reason}")

if __name__ == "__main__":
    main()
