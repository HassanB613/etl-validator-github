import os
import subprocess
from datetime import datetime
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time
import requests  # Add this for TestRail integration
import configparser

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

# Load TestRail configuration
config = configparser.ConfigParser()
config.read("testrail_config.ini")

TESTRAIL_URL = config["TestRail"]["url"]
TESTRAIL_USERNAME = config["TestRail"]["username"]
TESTRAIL_API_KEY = config["TestRail"]["api_key"]
TESTRAIL_RUN_ID = int(config["TestRail"]["run_id"])
TESTRAIL_TEST_ID = int(config["TestRail"]["test_id"])

# --------------------
# TestRail Configuration
# --------------------
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
def run_generator_file(is_valid=True, timestamp=None):
    """
    Generate test files with the naming convention:
    mtfdm_dev_dmbankdata_YYYYMMDD_HHMMSS.parquet
    """
    # Use the provided timestamp or generate a new one
    timestamp = timestamp or datetime.now().strftime("%Y%m%d.%H%M%S")
    output_dir = f"./test_output/{'valid' if is_valid else 'invalid'}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    # Use the new naming convention in lowercase
    output_filename = f"mtfdm_dev_dmbankdata_{timestamp}"

    cmd = [
        "python",
        "newaugsver.py",
        "--rows", "50",
        "--seed", "100",
        "--formats", "parquet",
        "--output-dir", output_dir,
        "--output", output_filename  # No .parquet here
    ]

    if not is_valid:
        cmd += ["--missing-data", "--missing-columns", "OrganizationTin", "ContactEmail"]

    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # Add .parquet extension to the full path
    full_path = os.path.join(output_dir, output_filename + ".parquet")
    return full_path

# --------------------
# Step 2: Upload to S3
# --------------------
def upload_to_s3(file_path):
    """
    Upload files to S3 with the new naming convention.
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

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
def check_s3_file_exists(prefix):
    """
    Check if a file exists in the S3 bucket with a specific prefix and exact timestamp.
    The search will match the full file name, including seconds (YYYYMMDD.HHMMSS).
    """
    print(f"🔍 Checking S3 prefix {prefix} for the exact file name...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    
    # Use the global RUN_TIMESTAMP
    expected_filename = f"DMBankErrorfile.{RUN_TIMESTAMP}.csv"
    
    for obj in result.get("Contents", []):
        # Check if the file name matches the expected file name
        if obj["Key"].endswith(expected_filename):
            print(f"✅ Found: {obj['Key']}")
            return True
    
    print(f"❌ No file found in {prefix} matching the exact file name ({expected_filename})")
    return False

def check_s3_file_exists(prefix, keyword):
    print(f"🔍 Checking S3 prefix {prefix} for file containing '{keyword}'...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    for obj in result.get("Contents", []):
        if keyword in obj["Key"]:
            print(f"✅ Found: {obj['Key']}")
            return True
    print(f"❌ No file found in {prefix} containing '{keyword}'")
    return False

def check_s3_file_exists_with_naming_convention(prefix, timestamp):
    """
    Check if a file exists in the S3 bucket with the new naming convention.
    Naming convention: mtfdm_dev_dmbankdata_YYYYMMDD_HHMMSS.parquet
    """
    print(f"🔍 Checking S3 prefix {prefix} for files matching the naming convention...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    
    # Generate the expected filename in lowercase
    expected_filename = f"mtfdm_dev_dmbankdata_{timestamp}.parquet"
    
    for obj in result.get("Contents", []):
        if obj["Key"].endswith(expected_filename):
            print(f"✅ Found: {obj['Key']}")
            return True
    
    # Change ❌ to ⭐ for success in Step 5
    print(f"⭐ No file found in {prefix} matching the naming convention ({expected_filename})")
    return False

# --------------------
# Main Test Orchestration
# --------------------
def main():
    try:
        # Initialize a dictionary to track step statuses
        step_status = {
            "Step 1": "Passed",
            "Step 2": "Passed",
            "Step 3": "Passed",
            "Step 4": "Passed",
            "Step 5": "Passed",
            "Step 6": "Passed",
            "Step 7": "Passed"
        }

        print(">>> Step 1: Generate test files")
        
        # Generate valid file with a unique timestamp
        valid_timestamp = datetime.now().strftime("%Y%m%d.%H%M%S")
        valid_file = run_generator_file(is_valid=True, timestamp=valid_timestamp)
        
        # Add a delay to ensure a unique timestamp for the invalid file
        time.sleep(5)  # Wait for 5 seconds
        
        # Generate invalid file with a new unique timestamp
        invalid_timestamp = datetime.now().strftime("%Y%m%d.%H%M%S")
        invalid_file = run_generator_file(is_valid=False, timestamp=invalid_timestamp)

        print(">>> Step 2: Upload to S3")
        upload_to_s3(valid_file)
        upload_to_s3(invalid_file)

        # Wait for files to propagate in S3
        time.sleep(5)

        print(">>> Step 3: Validate S3 outputs before triggering Glue job")
        valid_file_found = check_s3_file_exists_with_naming_convention(S3_PREFIX, valid_timestamp)
        invalid_file_found = check_s3_file_exists_with_naming_convention(S3_PREFIX, invalid_timestamp)

        # Assert that both files are present in the ready folder
        assert valid_file_found, f"❌ Valid file not found in S3: {valid_file}"
        assert invalid_file_found, f"❌ Invalid file not found in S3: {invalid_file}"
        print("✅ Both valid and invalid files are present in S3.")

        print(">>> Step 4: Trigger and monitor Glue job")
        glue_job_success = wait_for_glue_success(GLUE_JOB_NAME)
        if not glue_job_success:
            step_status["Step 4"] = "Failed"
            raise Exception("❌ Glue job failed.")
        
        time.sleep(20)  # Buffer time for outputs to land

        print(">>> Step 5: Validate S3 outputs (Ready folder)")
        try:
            valid_file_absent = not check_s3_file_exists_with_naming_convention(S3_PREFIX, valid_timestamp)
            invalid_file_absent = not check_s3_file_exists_with_naming_convention(S3_PREFIX, invalid_timestamp)

            assert valid_file_absent, f"❌ Valid file still found in S3 ready folder: {valid_timestamp}"
            assert invalid_file_absent, f"❌ Invalid file still found in S3 ready folder: {invalid_timestamp}"
            print("✅ Both valid and invalid files are no longer in the S3 ready folder.")
        except AssertionError as e:
            step_status["Step 5"] = f"Failed: {str(e)}"
            print(str(e))

        print(">>> Step 6: Validate S3 outputs (Archive folder)")
        try:
            current_year = datetime.now().strftime("%Y")
            current_month = datetime.now().strftime("%m")
            archive_prefix = f"bankfile/archive/{current_year}/{current_month}"

            valid_file_in_archive = check_s3_file_exists_with_naming_convention(archive_prefix, valid_timestamp)
            assert valid_file_in_archive, f"❌ Valid file not found in S3 archive folder: {valid_timestamp}"
            print("✅ Valid file successfully moved to the archive folder.")

            invalid_file_in_archive = check_s3_file_exists_with_naming_convention(archive_prefix, invalid_timestamp)
            assert invalid_file_in_archive, f"❌ Invalid file not found in S3 archive folder: {invalid_timestamp}"
            print("✅ Invalid file successfully moved to the archive folder.")
        except AssertionError as e:
            step_status["Step 6"] = f"Failed: {str(e)}"
            print(str(e))

        print(">>> Step 7: Validate S3 outputs (Error folder)")
        try:
            expected_error_csv = f"DMBankErrorfile.{invalid_timestamp}.csv"
            error_csv_found = check_s3_file_exists(ERROR_CSV_PREFIX, expected_error_csv)

            assert error_csv_found, f"❌ Error CSV file not found in S3 error folder: {expected_error_csv}"
            print(f"✅ Error CSV file successfully found in the error folder: {expected_error_csv}")
        except AssertionError as e:
            step_status["Step 7"] = f"Failed: {str(e)}"
            print(str(e))

        # Debugging output for step statuses
        print("\nDebugging Step Statuses:")
        for step, status in step_status.items():
            print(f"{step}: {status}")

    except AssertionError as e:
        print(str(e))
    except Exception as e:
        print(str(e))
    finally:
        # Report overall step statuses to TestRail
        detailed_comment = "\n".join([f"{step}: {status}" for step, status in step_status.items()])

        # Update failure detection logic
        overall_status = 5 if any("Failed" in str(status) for status in step_status.values()) else 1

        # Print overall test result in the terminal
        if overall_status == 5:
            print("❌ Overall Test Result: Failed")
        else:
            print("✅ Overall Test Result: Passed")

        # Report to TestRail
        report_to_testrail(TESTRAIL_TEST_ID, overall_status, detailed_comment)

# Global timestamp for the current script run
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d.%H%M%S")

if __name__ == "__main__":
    main()
