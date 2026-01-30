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
import argparse
import threading
import random  # Add this import for random choice
import re  # Add this import for regex
from botocore.exceptions import ClientError

# --------------------
# Configuration
# --------------------
BUCKET = "mtfpm-dev-s3-mtfdmstaging-us-east-1"  # <-- Update to your actual bucket name
BUCKET_2 = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"

GLUE_JOB_NAME = "mtfpm-bankfile-validation-error-handling-dev"
GLUE_JOB_NAME_2 = "load-bank-file-stg-dev2"

S3_PREFIX = "bankfile/ready"           # Update to the correct prefix
ARCHIVE_PREFIX = "bankfile/archive/2025/07"
ARCHIVE_PREFIX_2 = "bankfile/archive/2025/07"  # Dev2 archive prefix
ERROR_CSV_PREFIX = "bankfile/error/"
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")  # Global timestamp for filenames
ENV_SUFFIX = "dev2"

s3 = boto3.client("s3")
glue = boto3.client("glue", region_name="us-east-1")

# Load TestRail configuration
config = configparser.ConfigParser()
# Try multiple paths to find the config file
config_paths = [
    "testrail_config.ini",
    "../testrail_config.ini",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "testrail_config.ini")
]
config_found = False
for config_path in config_paths:
    if os.path.exists(config_path):
        config.read(config_path)
        config_found = True
        break

if config_found and "TestRail" in config:
    TESTRAIL_URL = config["TestRail"]["url"]
    TESTRAIL_USERNAME = config["TestRail"]["username"]
    TESTRAIL_API_KEY = config["TestRail"]["api_key"]
    TESTRAIL_RUN_ID = int(config["TestRail"]["run_id"])
    TESTRAIL_TEST_ID = int(config["TestRail"]["test_id"])
else:
    # Default values if config not found
    TESTRAIL_URL = ""
    TESTRAIL_USERNAME = ""
    TESTRAIL_API_KEY = ""
    TESTRAIL_RUN_ID = 0
    TESTRAIL_TEST_ID = 0
    print("Warning: testrail_config.ini not found. TestRail reporting will be skipped.")

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
    if not TESTRAIL_URL or not TESTRAIL_API_KEY:
        print("⚠️ TestRail not configured. Skipping test result reporting.")
        return
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
def run_generator_file(is_valid=True, timestamp=None, seed=None, rows=50):
    """
    Generate test files with the naming convention:
    mtfdm_dev_dmbankdata_YYYYMMDD_HHMMSS.parquet
    Also creates an Excel file in the same folder.
    """    # Use the provided timestamp or generate a new one
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/{'valid' if is_valid else 'invalid'}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    # Use the new naming convention in lowercase
    # Ensure timestamp uses underscore between date and time
    # If timestamp is in format YYYYMMDD.HHMMSS, replace dot with underscore
    if '.' in timestamp:
        timestamp = timestamp.replace('.', '_')
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"

    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", "parquet", "--output-dir", output_dir, "--output", output_filename]
    # include seed if provided
    if seed is not None:
        cmd += ["--seed", str(seed)]

    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # Add .parquet extension to the full path
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")    # Import pandas for both scenarios
    try:
        import pandas as pd
    except ImportError:
        print("⚠️ pandas not available, cannot create Excel/CSV files")
        return parquet_path

    # For invalid scenario, blank out OrganizationTIN and ContactEmail in the generated file
    if not is_valid:
        try:
            df = pd.read_parquet(parquet_path)
            if "OrganizationTIN" in df.columns:
                df["OrganizationTIN"] = ""
            if "ContactEmail" in df.columns:
                df["ContactEmail"] = ""
            df.to_parquet(parquet_path, index=False)
            # Also update Excel and CSV sidecars
            excel_path = os.path.join(output_dir, output_filename + ".xlsx")
            df.to_excel(excel_path, index=False)
            csv_path = os.path.join(output_dir, output_filename + ".csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Invalid scenario: blanked OrganizationTIN and ContactEmail in {parquet_path}")
        except Exception as e:
            print(f"⚠️ Could not blank columns for invalid scenario: {e}")
    else:
        # --- Create Excel file from the Parquet file ---
        try:
            df = pd.read_parquet(parquet_path)
            excel_path = os.path.join(output_dir, output_filename + ".xlsx")
            df.to_excel(excel_path, index=False)
            print(f"✅ Excel file created: {excel_path}")
            # Also generate CSV version of the parquet
            csv_path = os.path.join(output_dir, output_filename + ".csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ CSV file created: {csv_path}")
        except Exception as e:
            print(f"⚠️ Could not create Excel/CSV file: {e}")

    return parquet_path

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
    try:
        s3.upload_file(file_path, BUCKET, s3_key)
        print(f"✅ Successfully uploaded to s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"⚠️ S3 upload failed: {str(e)}")
        print(f"ℹ️ Continuing without AWS operations. Please configure AWS credentials if S3 upload is required.")
    return s3_key

# --------------------
# Step 3: Trigger & monitor Glue
# --------------------
def get_running_glue_job(job_name):
    """
    Check if the Glue job is already running.
    Returns the JobRunId if running, None otherwise.
    """
    try:
        # Get recent job runs (RUNNING or STARTING states)
        response = glue.get_job_runs(JobName=job_name, MaxResults=5)
        for run in response.get("JobRuns", []):
            state = run.get("JobRunState")
            if state in ["RUNNING", "STARTING", "WAITING"]:
                print(f"🔍 Found existing Glue job run: {run['Id']} (State: {state})")
                return run["Id"]
        return None
    except Exception as e:
        print(f"⚠️ Could not check for running Glue jobs: {e}")
        return None

def wait_for_glue_success(job_name, timeout=600):
    """
    Wait for Glue job to succeed. First checks if job is already running (auto-triggered by S3),
    if so, monitors that run. Otherwise starts a new run.
    """
    run_id = None
    
    # First, check if job is already running (may have been auto-triggered by S3 file upload)
    print("🔍 Checking if Glue job is already running...")
    existing_run_id = get_running_glue_job(job_name)
    
    if existing_run_id:
        print(f"✅ Glue job is already running (RunId: {existing_run_id}). Monitoring existing run...")
        run_id = existing_run_id
    else:
        print("🕒 No running Glue job found. Starting new Glue job...")
        attempt = 0
        # Retry on concurrency errors (in case job started between our check and start attempt)
        while True:
            attempt += 1
            try:
                response = glue.start_job_run(JobName=job_name)
                run_id = response["JobRunId"]
                print(f"✅ Started new Glue job run: {run_id}")
                break
            except Exception as e:
                # Determine if this is a concurrency exception
                error_code = None
                if isinstance(e, ClientError):
                    error_code = e.response.get("Error", {}).get("Code")
                elif "ConcurrentRunsExceededException" in str(e):
                    error_code = "ConcurrentRunsExceededException"
                if error_code == "ConcurrentRunsExceededException":
                    # Job started between our check and start - find it and monitor
                    print(f"⚠️ Concurrent run detected. Checking for the running job...")
                    time.sleep(5)
                    existing_run_id = get_running_glue_job(job_name)
                    if existing_run_id:
                        print(f"✅ Found running job: {existing_run_id}. Monitoring it...")
                        run_id = existing_run_id
                        break
                    # If still no running job found, retry start
                    wait_time = min(60, 10 * attempt)
                    print(f"⏳ Retrying in {wait_time} seconds (attempt {attempt})...")
                    time.sleep(wait_time)
                    continue
                print(f"❌ Error starting Glue job: {e}")
                return False
    
    if not run_id:
        print("❌ Could not start or find Glue job.")
        return False

    # Monitor the job run
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            status = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
            print(f"⌛ Glue job status: {status}")
            if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                if status == "SUCCEEDED":
                    print("✅ Glue job succeeded. Waiting 45 seconds for S3 propagation...")
                    time.sleep(45)
                return status == "SUCCEEDED"
        except Exception as e:
            print(f"⚠️ Error checking job status: {e}")
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
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        
        # Use the global RUN_TIMESTAMP
        expected_filename = f"DMBankErrorfile.{RUN_TIMESTAMP}.csv"
        
        for obj in result.get("Contents", []):
            # Check if the file name matches the expected file name
            if obj["Key"].endswith(expected_filename):
                print(f"✅ Found: {obj['Key']}")
                return True
        
        print(f"❌ No file found in {prefix} matching the exact file name ({expected_filename})")
    except Exception as e:
        print(f"⚠️ Failed to check S3 file: {str(e)}")
        print(f"ℹ️ Skipping S3 check. Please configure AWS credentials if S3 access is required.")
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
    expected_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}.parquet"
    
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
def get_user_choice_with_timeout(timeout=30):
    """
    Prompt the user to select 'valid' or 'invalid' scenario.
    If no input is received within 'timeout' seconds, default to 'valid'.
    """
    choice = {'value': None}

    def ask():
        print(f"Select scenario to test ('valid' or 'invalid') [default: valid in {timeout} seconds]: ", end='', flush=True)
        user_input = input().strip().lower()
        if user_input in ['valid', 'invalid']:
            choice['value'] = user_input

    thread = threading.Thread(target=ask)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    return choice['value'] if choice['value'] else 'valid'

def download_s3_folder_to_local(s3_prefix, local_evidence_dir, timestamp=None):
    files_downloaded = 0  # initialize download counter to avoid unbound error
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)
        for obj in result.get("Contents", []):
            key = obj["Key"]
            if timestamp and timestamp not in key:
                continue
            # Skip folder marker
            if key.endswith("/") or key == s3_prefix:
                continue
            # Create evidence dir if not already created
            if files_downloaded == 0:
                os.makedirs(local_evidence_dir, exist_ok=True)
            local_path = os.path.join(local_evidence_dir, os.path.basename(key))
            print(f"⬇️ Downloading {key} to {local_path}")
            s3.download_file(BUCKET, key, local_path)
            files_downloaded += 1
        if files_downloaded == 0:
            print(f"ℹ️ No files found in s3://{BUCKET}/{s3_prefix} to download.")
        else:
            print(f"✅ Downloaded {files_downloaded} file(s) from s3://{BUCKET}/{s3_prefix} to {local_evidence_dir}")
    except Exception as e:
        print(f"⚠️ Failed to download S3 folder: {str(e)}")
        print(f"ℹ️ Skipping S3 download. Please configure AWS credentials if S3 access is required.")
    return files_downloaded

def save_s3_listing_to_file(s3_prefix, local_evidence_dir, listing_filename):
    """
    Save a text snapshot of the S3 folder contents (key, size, last_modified) to a file in the evidence directory.
    """
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)
        lines = []
        for obj in result.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or key == s3_prefix:
                continue
            size = obj.get("Size", "?")
            last_modified = obj.get("LastModified", "?")
            lines.append(f"{key}\t{size}\t{last_modified}")
        if lines:
            os.makedirs(local_evidence_dir, exist_ok=True)
            listing_path = os.path.join(local_evidence_dir, listing_filename)
            with open(listing_path, "w", encoding="utf-8") as f:
                f.write("Key\tSize\tLastModified\n")
                f.write("\n".join(lines))
            print(f"📝 S3 listing saved to {listing_path}")
        else:
            print(f"ℹ️ No files in s3://{BUCKET}/{s3_prefix} to list.")
    except Exception as e:
        print(f"⚠️ Failed to save S3 listing: {str(e)}")
        print(f"ℹ️ Skipping S3 listing. Please configure AWS credentials if S3 access is required.")

def safe_s3_evidence_collection(s3_prefix, local_evidence_dir, listing_filename, download_func):
    """
    Safely collect S3 evidence: download files and save listing, handling S3 errors gracefully.
    Returns the number of files downloaded (0 if none or on error).
    """
    try:
        files_downloaded = download_func(s3_prefix, local_evidence_dir)
        save_s3_listing_to_file(s3_prefix, local_evidence_dir, listing_filename)
        return files_downloaded
    except Exception as e:
        print(f"⚠️ S3 evidence collection failed for {s3_prefix}: {e}")
        return 0

def run_test_scenario(file_type, seed=None, rows=50):
    step_status = {
        "Step 1": "Passed",
        "Step 2": "Passed",
        "Step 3": "Passed",
        "Step 4": "Passed",
        "Step 5": "Passed",
        "Step 6": "Passed",
        "Step 7": "Passed"
    }
    file_path = None  # Initialize file_path
    try:
        print(f"\n>>> Running test for scenario: {file_type.upper()}")
        timestamp = datetime.now().strftime("%Y%m%d.%H%M%S")
        is_valid = file_type == "valid"
        file_path = run_generator_file(is_valid=is_valid, timestamp=timestamp, seed=seed, rows=rows)

        print(">>> Step 2: Upload to S3")
        upload_to_s3(file_path)

        time.sleep(5)

        print(">>> Step 3: Validate S3 outputs before triggering Glue job")
        file_found = check_s3_file_exists_with_naming_convention(S3_PREFIX, timestamp)
        assert file_found, f"❌ {file_type.capitalize()} file not found in S3: {file_path}"
        print(f"✅ {file_type.capitalize()} file is present in S3.")

        print(">>> Step 4: Trigger and monitor Glue job")
        glue_job_success = wait_for_glue_success(GLUE_JOB_NAME)
        if not glue_job_success:
            step_status["Step 4"] = "Failed"
            raise Exception("❌ Glue job failed.")

        # Additional wait for S3 consistency after Glue completes
        print("⏳ Waiting additional 30 seconds for S3 consistency...")
        time.sleep(30)

        print(">>> Step 5: Validate S3 outputs (Ready folder)")
        try:
            # Retry up to 3 times with 15-second intervals for S3 consistency
            file_absent = False
            for retry in range(3):
                file_absent = not check_s3_file_exists_with_naming_convention(S3_PREFIX, timestamp)
                if file_absent:
                    break
                print(f"⏳ File still in ready folder, waiting 15s (retry {retry + 1}/3)...")
                time.sleep(15)
            assert file_absent, f"❌ {file_type.capitalize()} file still found in S3 ready folder: {timestamp}"
            print(f"✅ {file_type.capitalize()} file is no longer in the S3 ready folder.")
        except AssertionError as e:
            step_status["Step 5"] = f"Failed: {str(e)}"
            print(str(e))
        print(">>> Step 6: Validate S3 outputs (Archive folder)")
        try:
            current_year = datetime.now().strftime("%Y")
            current_month = datetime.now().strftime("%m")
            archive_prefix = f"bankfile/archive/{current_year}/{current_month}"
            # Retry up to 3 times with 15-second intervals for S3 consistency
            file_in_archive = False
            for retry in range(3):
                file_in_archive = check_s3_file_exists_with_naming_convention(archive_prefix, timestamp)
                if file_in_archive:
                    break
                print(f"⏳ File not yet in archive folder, waiting 15s (retry {retry + 1}/3)...")
                time.sleep(15)
            assert file_in_archive, f"❌ {file_type.capitalize()} file not found in S3 archive folder: {timestamp}"
            print(f"✅ {file_type.capitalize()} file successfully moved to the archive folder.")
        except AssertionError as e:
            step_status["Step 6"] = f"Failed: {str(e)}"
            print(str(e))

        print(">>> Step 7: Validate S3 outputs (Error folder)")
        try:
            if is_valid:
                # For valid scenario, fail if ANY real error file exists in the error folder
                try:
                    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=ERROR_CSV_PREFIX)
                    error_files = [
                        obj["Key"] for obj in result.get("Contents", [])
                        if obj["Key"] != ERROR_CSV_PREFIX  # Ignore the folder marker
                    ]
                    assert not error_files, f"❌ Unexpected error file(s) found in S3 error folder: {error_files}"
                    print(f"✅ No error file found in the error folder for the valid scenario (as expected).")
                except Exception as e:
                    print(f"⚠️ Could not verify S3 error folder: {str(e)}")
                    print(f"ℹ️ Skipping error folder validation. Please configure AWS credentials if S3 access is required.")
            else:
                # For invalid scenario, check for the newest error file matching the date
                error_prefix = f"mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}"  # matches mtfdm_dev2_dmbankerrorfile_YYYYMMDD_HHMMSS
                newest_error_file = get_newest_error_file(ERROR_CSV_PREFIX, error_prefix)
                assert newest_error_file, f"❌ No error file found in S3 error folder with prefix: {error_prefix}"
                print(f"✅ Newest error file for this run: {newest_error_file}")
        except AssertionError as e:
            step_status["Step 7"] = f"Failed: {str(e)}"
            print(str(e))

        print("\nDebugging Step Statuses:")
        for step, status in step_status.items():
            print(f"{step}: {status}")

    except AssertionError as e:
        print(str(e))
    except Exception as e:
        print(str(e))
    finally:
        # --- Download S3 evidence BEFORE TestRail reporting ---
        if file_path:  # Only proceed if file_path was set
            test_output_dir = os.path.dirname(file_path)
            evidence_dir = os.path.join(test_output_dir, "test evidence s3 ready folder")
            print(f"\n>>> Collecting S3 evidence to: {evidence_dir}")
            
            # Download only the specific archive file (same name as uploaded)
            current_year = datetime.now().strftime("%Y")
            current_month = datetime.now().strftime("%m")
            archive_prefix = f"bankfile/archive/{current_year}/{current_month}"
            archive_found, archive_downloaded = download_specific_archive_file(archive_prefix, timestamp, evidence_dir)
            
            # Download only the newest error file (for invalid scenarios)
            error_prefix = f"mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}"  # matches mtfdm_dev2_dmbankerrorfile_YYYYMMDD_HHMMSS
            error_files = download_newest_error_file_to_local(ERROR_CSV_PREFIX, evidence_dir, error_prefix)
            
            # Save S3 listings for reference
            save_s3_listing_to_file(ERROR_CSV_PREFIX, evidence_dir, "s3_error_listing_before_delete.txt")
            
            if not archive_downloaded and error_files == 0:
                if os.path.exists(evidence_dir) and not os.listdir(evidence_dir):
                    os.rmdir(evidence_dir)
                    print(f"🗑️ Removed empty evidence directory: {evidence_dir}")
        # --- Now report to TestRail ---
        detailed_comment = f"Scenario: {file_type}\n" + "\n".join([f"{step}: {status}" for step, status in step_status.items()])
        overall_status = 5 if any("Failed" in str(status) for status in step_status.values()) else 1
        if overall_status == 5:
            print("❌ Overall Test Result: Failed")
        else:
            print("✅ Overall Test Result: Passed")
        report_to_testrail(TESTRAIL_TEST_ID, overall_status, detailed_comment)
        return step_status, overall_status

# --- Refactor scenario functions to return file_path and timestamp ---
def run_missing_column_scenario(column_names, rows=50, timestamp=None):
    # Allow column_names to be either a list or a comma-separated string
    if isinstance(column_names, str):
        column_list = [col.strip() for col in column_names.split(',')]
    else:
        column_list = column_names
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/missing_columns_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = [
        "python",
        "newaugsver_clean.py",
        "--rows", str(rows),
        "--formats", "parquet",
        "--output-dir", output_dir,
        "--output", output_filename
    ]
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    try:
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        for col in column_list:
            if col in df.columns:
                df = df.drop(columns=[col])
        excel_path = os.path.join(output_dir, output_filename + ".xlsx")
        df.to_excel(excel_path, index=False)
        print(f"✅ Excel file created: {excel_path}")
        csv_path = os.path.join(output_dir, output_filename + ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV file created: {csv_path}")
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Parquet file updated with missing columns: {column_list}")
    except Exception as e:
        print(f"⚠️ Could not create Excel/CSV/Parquet file: {e}")
    try:
        upload_to_s3(parquet_path)
        print(f"✅ Uploaded file missing column(s): {column_names}")
    except Exception as e:
        print(f"❌ Could not upload to S3: {e}\nSkipping full ETL pipeline.")
        return None, timestamp
    # Run full ETL pipeline if upload succeeded
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name=f"missing_column_{column_names}", timestamp=timestamp)

def run_rename_column_scenario(rename_specs, rows=50, timestamp=None):
    """Generate dataset then rename one or more columns before running full ETL.

    rename_specs can be:
      - a single string "OldName:NewName"
      - a list of strings ["Old1:New1", "Old2:New2"]
    """
    if isinstance(rename_specs, str):
        rename_list = [rename_specs]
    else:
        rename_list = rename_specs
    mappings = {}
    for spec in rename_list:
        if ':' not in spec:
            print(f"⚠️ Invalid rename spec '{spec}' (expected OldName:NewName). Skipping.")
            continue
        old, new = spec.split(':', 1)
        old = old.strip()
        new = new.strip()
        if old and new:
            mappings[old] = new
    if not mappings:
        print("❌ No valid rename mappings provided. Aborting rename scenario.")
        return None, timestamp
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/rename_columns_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = [
        "python",
        "newaugsver_clean.py",
        "--rows", str(rows),
        "--formats", "parquet",
        "--output-dir", output_dir,
        "--output", output_filename
    ]
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    try:
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        applied = {}
        for old, new in mappings.items():
            if old in df.columns:
                if new in df.columns:
                    print(f"⚠️ Target name '{new}' already exists. Skipping rename of '{old}'.")
                else:
                    df = df.rename(columns={old: new})
                    applied[old] = new
            else:
                print(f"⚠️ Column '{old}' not found. Skipping.")
        if not applied:
            print("❌ No column names were changed. Aborting downstream run.")
            return None, timestamp
        excel_path = os.path.join(output_dir, output_filename + ".xlsx")
        df.to_excel(excel_path, index=False)
        print(f"✅ Excel file created: {excel_path}")
        csv_path = os.path.join(output_dir, output_filename + ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV file created: {csv_path}")
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Parquet file updated with renamed columns: {applied}")
    except Exception as e:
        print(f"⚠️ Could not create updated files after rename: {e}")
    try:
        upload_to_s3(parquet_path)
        print(f"✅ Uploaded file with renamed columns: {applied}")
    except Exception as e:
        print(f"❌ Could not upload to S3: {e}\nSkipping full ETL pipeline.")
        return None, timestamp
    # Run full ETL pipeline if upload succeeded
    scenario_tag = "_".join([f"{o}2{n}" for o, n in mappings.items()])
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name=f"rename_column_{scenario_tag}", timestamp=timestamp)

def run_rename_and_invalid_values_scenario(rename_specs, invalid_values, rows=50, formats=["parquet"], seed=None, extra_args=None, timestamp=None):
    """Composite scenario: rename columns first, then inject invalid values.

    rename_specs: list like ["Old:New", ...]
    invalid_values: list like ["Col:BadVal", "Col:row_index=value", ...]
    Invalid values will try to match renamed columns; if a spec references the original name
    it will map through the rename mapping automatically.
    """
    if isinstance(rename_specs, str):
        rename_list = [rename_specs]
    else:
        rename_list = rename_specs or []
    mappings = {}
    for spec in rename_list:
        if ':' not in spec:
            print(f"⚠️ Invalid rename spec '{spec}' (expected Old:New). Skipping.")
            continue
        old, new = spec.split(':', 1)
        old = old.strip(); new = new.strip()
        if old and new:
            mappings[old] = new
    if not mappings:
        print("⚠️ No valid rename mappings provided; continuing with invalid value injection only.")
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/rename_invalid_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats,
           "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    try:
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        applied = {}
        # Apply renames
        for old, new in mappings.items():
            if old in df.columns:
                if new in df.columns:
                    print(f"⚠️ Target name '{new}' already exists. Skipping rename of '{old}'.")
                else:
                    df = df.rename(columns={old: new})
                    applied[old] = new
            else:
                print(f"⚠️ Column '{old}' not found for rename.")
        inverse_map = {o: n for o, n in applied.items()}
        # Inject invalid values after rename
        for item in invalid_values or []:
            if ':' not in item:
                continue
            col, val = item.split(':', 1)
            col = col.strip(); val = val.strip()
            target_col = col
            # If original name was renamed, map to new
            if col not in df.columns and col in inverse_map:
                target_col = inverse_map[col]
            if target_col in df.columns:
                if '=' in val:
                    try:
                        row_index, value = val.split('=', 1)
                        row_index = int(row_index.strip())
                        value = value.strip()
                        if 0 <= row_index < len(df):
                            df.at[row_index, target_col] = value
                    except Exception as e:
                        print(f"⚠️ Could not apply row-specific invalid value '{item}': {e}")
                else:
                    df[target_col] = val
            else:
                print(f"⚠️ Column '{col}' (mapped to '{target_col}') not found for invalid value injection.")
        # Save updated artifacts
        df.to_parquet(parquet_path, index=False)
        excel_path = os.path.join(output_dir, output_filename + ".xlsx"); df.to_excel(excel_path, index=False)
        csv_path = os.path.join(output_dir, output_filename + ".csv"); df.to_csv(csv_path, index=False)
        print(f"✅ Applied renames {applied} and invalid values {invalid_values}.")
    except Exception as e:
        print(f"❌ Failed during rename+invalid processing: {e}")
        return None, timestamp
    try:
        upload_to_s3(parquet_path)
        print("✅ Uploaded composite scenario file to S3")
    except Exception as e:
        print(f"❌ Upload failed: {e}\nSkipping ETL run.")
        return None, timestamp
    tag_parts = [f"{o}2{n}" for o, n in mappings.items()] if mappings else ["noRename"]
    tag = "_".join(tag_parts)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name=f"rename_invalid_{tag}", timestamp=timestamp)

def run_composite_transform_scenario(rename_specs=None, invalid_values=None, drop_columns=None, rows=50, formats=["parquet"], seed=None, extra_args=None, timestamp=None):
    """Composite scenario applying (in order): generate -> rename -> drop columns -> inject invalid values.

    rename_specs: ["Old:New", ...]
    invalid_values: ["Col:BadVal", "Col:row=value", ...]
    drop_columns: ["Col1", "Col2", ...]
    Order rationale:
      1. Rename first so drop & invalid operations can reference either original or new names.
      2. Drop columns before injecting invalid values (can't inject into dropped columns).
      3. Map invalid value column names through rename mapping if needed.
    """
    if isinstance(rename_specs, str):
        rename_list = [rename_specs]
    else:
        rename_list = rename_specs or []
    if isinstance(drop_columns, str):
        drop_list = [c.strip() for c in drop_columns.split(',') if c.strip()]
    else:
        drop_list = drop_columns or []
    if isinstance(invalid_values, str):
        invalid_list = [invalid_values]
    else:
        invalid_list = invalid_values or []
    mappings = {}
    for spec in rename_list:
        if ':' not in spec:
            print(f"⚠️ Invalid rename spec '{spec}' (expected Old:New). Skipping.")
            continue
        old, new = spec.split(':', 1)
        old = old.strip(); new = new.strip()
        if old and new:
            mappings[old] = new
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/composite_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats, "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    applied = {}
    try:
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        # 1. Rename
        for old, new in mappings.items():
            if old in df.columns:
                if new in df.columns:
                    print(f"⚠️ Target name '{new}' already exists. Skipping rename of '{old}'.")
                else:
                    df = df.rename(columns={old: new})
                    applied[old] = new
            else:
                print(f"⚠️ Column '{old}' not found for rename.")
        # Build mapping from original -> final for invalid injection
        name_map = {o: applied.get(o, o) for o in set(list(df.columns) + list(mappings.keys()))}
        # 2. Drop columns (accept either original or renamed names)
        actually_dropped = []
        for col in drop_list:
            # If user specified original name that was renamed, map to new name
            target = applied.get(col, col)
            if target in df.columns:
                df = df.drop(columns=[target])
                actually_dropped.append(target)
            else:
                print(f"⚠️ Drop column '{col}' (mapped '{target}') not found.")
        # 3. Inject invalid values (skip dropped columns)
        for item in invalid_list:
            if ':' not in item:
                continue
            col, val = item.split(':', 1)
            col = col.strip(); val = val.strip()
            # Map through rename if needed
            target_col = applied.get(col, col)
            if target_col in actually_dropped:
                print(f"⚠️ Skipping invalid value for dropped column '{target_col}'.")
                continue
            if target_col in df.columns:
                if '=' in val:
                    try:
                        row_index, value = val.split('=', 1)
                        row_index = int(row_index.strip())
                        value = value.strip()
                        if 0 <= row_index < len(df):
                            df.at[row_index, target_col] = value
                    except Exception as e:
                        print(f"⚠️ Could not apply row-specific invalid value '{item}': {e}")
                else:
                    df[target_col] = val
            else:
                print(f"⚠️ Column '{col}' (mapped '{target_col}') not present for invalid injection.")
        # Save outputs
        df.to_parquet(parquet_path, index=False)
        df.to_excel(os.path.join(output_dir, output_filename + ".xlsx"), index=False)
        df.to_csv(os.path.join(output_dir, output_filename + ".csv"), index=False)
        print(f"✅ Composite applied. Renamed: {applied}. Dropped: {actually_dropped}. Invalid: {invalid_list}.")
    except Exception as e:
        print(f"❌ Composite processing failed: {e}")
        return None, timestamp
    try:
        upload_to_s3(parquet_path)
        print("✅ Uploaded composite scenario file to S3")
    except Exception as e:
        print(f"❌ Upload failed: {e}\nSkipping ETL run.")
        return None, timestamp
    tag_parts = []
    if applied:
        tag_parts.append("ren" + "_".join([f"{o}2{n}" for o, n in applied.items()]))
    if drop_list:
        tag_parts.append("drop" + "_".join(drop_list))
    if invalid_list:
        tag_parts.append("inv")
    tag = "__".join(tag_parts) if tag_parts else "composite"
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name=f"composite_{tag}", timestamp=timestamp)

def run_duplicate_row_scenario(row_index=0, rows=50, timestamp=None):
    """
    Generate a dataset, duplicate a specific row, and save the outputs.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/duplicate_row_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = [
        "python",
        "newaugsver_clean.py",
        "--rows", str(rows),
        "--seed", "246",
        "--formats", "csv", "xlsx", "parquet",
        "--output-dir", output_dir,
        "--output", output_filename
    ]

    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    df = pd.read_parquet(parquet_path)

    if 0 <= row_index < len(df):
        duplicate_row = df.iloc[[row_index]].copy()
        df = pd.concat([df, duplicate_row], ignore_index=True)
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Duplicated row {row_index} in file: {parquet_path}")
    else:
        print(f"⚠️ Row index {row_index} is out of bounds; no duplication performed.")

    try:
        csv_path = os.path.join(output_dir, output_filename + ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV file created: {csv_path}")

        excel_path = os.path.join(output_dir, output_filename + ".xlsx")
        df.to_excel(excel_path, index=False)
        print(f"✅ Excel file created: {excel_path}")
    except Exception as e:
        print(f"⚠️ Could not create Excel/CSV file: {e}")

    try:
        upload_to_s3(parquet_path)
        print(f"✅ Uploaded file with duplicated row {row_index}")
    except Exception as e:
        print(f"❌ Could not upload to S3: {e}")
        return None, timestamp

    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name=f"duplicate_row_{row_index}", timestamp=timestamp)

# New scenario: duplicate PayeeID across two rows
import re

def run_duplicate_payee_id_scenario(rows=50, formats=["parquet"], seed=None, timestamp=None):
    """
    Generate test file, then duplicate PayeeID across two rows and validate.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/duplicate_payee_id_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats, "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # Duplicate PayeeID
    df = pd.read_parquet(parquet_path)
    if len(df) >= 2:
        pid = str(df.at[0, "PayeeID"])
        df.at[1, "PayeeID"] = pid
        # adjust OrganizationIdentifier for second row
        num_match = re.search(r"(\d+)$", pid)
        if num_match:
            num = num_match.group(1)
            code = df.at[1, "OrganizationCode"]
            if code in ORG_IDENTIFIER_LAMBDA:
                df.at[1, "OrganizationIdentifier"] = ORG_IDENTIFIER_LAMBDA[code](num)
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Duplicated PayeeID across rows 0 and 1 in file: {parquet_path}")
    else:
        print("⚠️ Not enough rows to duplicate PayeeID.")
    # Create sidecars
    try:
        df.to_excel(os.path.join(output_dir, output_filename + ".xlsx"), index=False)
        df.to_csv(os.path.join(output_dir, output_filename + ".csv"), index=False)
    except Exception:
        pass
    # Upload and run full pipeline
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="duplicate_payee_id", timestamp=timestamp)

def get_newest_error_file(prefix, keyword):
    """
    Return the S3 key of the newest error file in the error folder matching the keyword (date prefix),
    using S3 LastModified (UTC) as the only criteria. This avoids timezone issues with filename timestamps.
    """
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        error_files = [
            obj for obj in result.get("Contents", [])
            if keyword in obj["Key"] and obj["Key"].endswith(".csv")
        ]
        if not error_files:
            print(f"❌ No error files found in {prefix} containing '{keyword}'")
            return None
        # Sort strictly by S3 LastModified (UTC)
        error_files.sort(key=lambda x: x["LastModified"], reverse=True)
        newest = error_files[0]["Key"]
        print(f"✅ Newest error file found by S3 LastModified: {newest}")
        return newest
    except Exception as e:
        print(f"⚠️ Failed to get newest error file: {str(e)}")
        print(f"ℹ️ Skipping error file retrieval. Please configure AWS credentials if S3 access is required.")
        return None

def download_newest_error_file_to_local(s3_prefix, local_evidence_dir, keyword):
    """
    Download only the newest error file (by S3 LastModified) matching the keyword to the evidence directory.
    Returns 1 if a file was downloaded, 0 otherwise.
    """
    try:
        newest_error_file = get_newest_error_file(s3_prefix, keyword)
        if not newest_error_file:
            print(f"ℹ️ No error file to download for keyword '{keyword}'")
            return 0
        os.makedirs(local_evidence_dir, exist_ok=True)
        local_path = os.path.join(local_evidence_dir, os.path.basename(newest_error_file))
        print(f"⬇️ Downloading newest error file {newest_error_file} to {local_path}")
        s3.download_file(BUCKET, newest_error_file, local_path)
        return 1
    except Exception as e:
        print(f"⚠️ Failed to download error file: {str(e)}")
        print(f"ℹ️ Skipping error file download. Please configure AWS credentials if S3 access is required.")
        return 0

def download_specific_archive_file(archive_prefix, timestamp, local_evidence_dir):
    """
    Download only the specific file from archive folder that matches our uploaded filename.
    Returns (found, downloaded) tuple - found=True if file exists, downloaded=True if successfully downloaded.
    """
    expected_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}.parquet"
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=archive_prefix)
        for obj in result.get("Contents", []):
            if obj["Key"].endswith(expected_filename):
                # Found the file - download it
                os.makedirs(local_evidence_dir, exist_ok=True)
                local_path = os.path.join(local_evidence_dir, os.path.basename(obj["Key"]))
                print(f"⬇️ Downloading archive file {obj['Key']} to {local_path}")
                s3.download_file(BUCKET, obj["Key"], local_path)
                print(f"✅ Successfully downloaded archive file: {expected_filename}")
                return True, True
        print(f"❌ Archive file not found: {expected_filename}")
        return False, False
    except Exception as e:
        print(f"⚠️ Failed to download archive file: {str(e)}")
        return False, False

def run_full_etl_pipeline_with_existing_file(file_path, scenario_name, timestamp):
    """
    Run the full ETL pipeline (Glue, S3 evidence, TestRail) for a file already generated and uploaded.
    """
    step_status = {
        "Step 1": "Passed (file generated)",
        "Step 2": "Passed (uploaded)",
        "Step 3": "Pending",
        "Step 4": "Pending",
        "Step 5": "Pending",
        "Step 6": "Pending",
        "Step 7": "Pending"
    }
    file_type = scenario_name
    try:
        print(f"\n>>> Running full ETL pipeline for scenario: {scenario_name}")
        print(">>> Step 3: Validate S3 outputs before triggering Glue job")
        file_found = check_s3_file_exists_with_naming_convention(S3_PREFIX, timestamp)
        assert file_found, f"❌ File not found in S3: {file_path}"
        print(f"✅ File is present in S3.")
        step_status["Step 3"] = "Passed"

        print(">>> Step 4: Trigger and monitor Glue job")
        glue_job_success = wait_for_glue_success(GLUE_JOB_NAME)
        if not glue_job_success:
            step_status["Step 4"] = "Failed"
            raise Exception("❌ Glue job failed.")
        step_status["Step 4"] = "Passed"
        time.sleep(20)

        print(">>> Step 5: Validate S3 outputs (Ready folder)")
        try:
            file_absent = not check_s3_file_exists_with_naming_convention(S3_PREFIX, timestamp)
            assert file_absent, f"❌ File still found in S3 ready folder: {timestamp}"
            print(f"✅ File is no longer in the S3 ready folder.")
            step_status["Step 5"] = "Passed"
        except AssertionError as e:
            step_status["Step 5"] = f"Failed: {str(e)}"
            print(str(e))

        print(">>> Step 6: Validate S3 outputs (Archive folder)")
        try:
            current_year = datetime.now().strftime("%Y")
            current_month = datetime.now().strftime("%m")
            archive_prefix = f"bankfile/archive/{current_year}/{current_month}"
            file_in_archive = check_s3_file_exists_with_naming_convention(archive_prefix, timestamp)
            assert file_in_archive, f"❌ File not found in S3 archive folder: {timestamp}"
            print(f"✅ File successfully moved to the archive folder.")
            step_status["Step 6"] = "Passed"
        except AssertionError as e:
            step_status["Step 6"] = f"Failed: {str(e)}"
            print(str(e))

        print(">>> Step 7: Validate S3 outputs (Error folder)")
        try:
            # For these scenarios, expect error file to exist
            error_prefix = f"mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}"  # matches mtfdm_dev2_dmbankerrorfile_YYYYMMDD_HHMMSS
            newest_error_file = get_newest_error_file(ERROR_CSV_PREFIX, error_prefix)
            assert newest_error_file, f"❌ No error file found in S3 error folder with prefix: {error_prefix}"
            print(f"✅ Newest error file for this run: {newest_error_file}")
            step_status["Step 7"] = "Passed"
        except AssertionError as e:
            step_status["Step 7"] = f"Failed: {str(e)}"
            print(str(e))

    except Exception as e:
        print(str(e))
    finally:
        # --- Download S3 evidence BEFORE TestRail reporting ---
        if file_path:  # Only proceed if file_path was set
            test_output_dir = os.path.dirname(file_path)
            evidence_dir = os.path.join(test_output_dir, "test evidence s3 ready folder")
            print(f"\n>>> Collecting S3 evidence to: {evidence_dir}")
            
            # Download only the specific archive file (same name as uploaded)
            current_year = datetime.now().strftime("%Y")
            current_month = datetime.now().strftime("%m")
            archive_prefix = f"bankfile/archive/{current_year}/{current_month}"
            archive_found, archive_downloaded = download_specific_archive_file(archive_prefix, timestamp, evidence_dir)
            
            # Download only the newest error file
            error_prefix = f"mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}"  # matches mtfdm_dev2_dmbankerrorfile_YYYYMMDD_HHMMSS
            error_files = download_newest_error_file_to_local(ERROR_CSV_PREFIX, evidence_dir, error_prefix)
            
            # Save S3 listings for reference
            save_s3_listing_to_file(ERROR_CSV_PREFIX, evidence_dir, "s3_error_listing_before_delete.txt")
            
            if not archive_downloaded and error_files == 0:
                if os.path.exists(evidence_dir) and not os.listdir(evidence_dir):
                    os.rmdir(evidence_dir)
                    print(f"🗑️ Removed empty evidence directory: {evidence_dir}")
        # --- Now report to TestRail ---
        detailed_comment = f"Scenario: {file_type}\n" + "\n".join([f"{step}: {status}" for step, status in step_status.items()])
        overall_status = 5 if any("Failed" in str(status) for status in step_status.values()) else 1
        if overall_status == 5:
            print("❌ Overall Test Result: Failed")
        else:
            print("✅ Overall Test Result: Passed")
        report_to_testrail(TESTRAIL_TEST_ID, overall_status, detailed_comment)
        return step_status, overall_status

def run_invalid_extension_scenario(extension, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file with an invalid extension, upload, and run full ETL validation.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/invalid_extension_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = [
        "python", "newaugsver_clean.py",
        "--rows", str(rows),
        "--formats", *formats,
        "--output-dir", output_dir,
        "--output", output_filename,
        "--invalid-extension", extension
    ]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    # Determine path for the generated file with the invalid extension
    invalid_file = output_filename + f".{extension}"
    file_path = os.path.join(output_dir, invalid_file)
    print(f"📤 Uploading invalid file {file_path} to S3 (should be rejected)")
    upload_to_s3(file_path)
    # After uploading the invalid extension file, run full ETL pipeline for error handling
    return run_full_etl_pipeline_with_existing_file(file_path, scenario_name="invalid_extension", timestamp=timestamp)

def run_invalid_mfr_ein_ssn_scenario(flag_value, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file with invalid Manufacturer EIN/SSN, upload, and run full ETL validation.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/invalid_mfr_ein_ssn_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    cmd = [
        "python", "newaugsver_clean.py",
        "--rows", str(rows),
        "--formats", *formats,
        "--output-dir", output_dir,
        "--output", output_filename,
        "--invalid-mfr-ein-ssn", flag_value
    ]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # create sidecars
    try:
        df = pd.read_parquet(parquet_path)
        df.to_excel(os.path.join(output_dir, output_filename + ".xlsx"), index=False)
        df.to_csv(os.path.join(output_dir, output_filename + ".csv"), index=False)
    except Exception:
        pass
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="invalid_mfr_ein_ssn", timestamp=timestamp)

def run_invalid_values_scenario(invalid_values, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file, inject invalid values into columns, upload, and run full ETL validation.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/invalid_values_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    # Ensure parquet output for sidecar (Excel/CSV) creation
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats,
           "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # Inject invalid values into the DataFrame
    try:
        import pandas as pd
        df = pd.read_parquet(parquet_path)
        for item in invalid_values:
            if ":" in item:
                col, val = item.split(":", 1)
                col = col.strip()
                val = val.strip()
                if col in df.columns:
                    # Check if specific rows are targeted (e.g., "ColumnName:row_index=value")
                    if "=" in val:
                        row_index, value = val.split("=", 1)
                        row_index = int(row_index.strip())
                        value = value.strip()
                        if 0 <= row_index < len(df):
                            df.at[row_index, col] = value
                    else:
                        # Replace the entire column with the invalid value
                        df[col] = val
        # Save updated files
        df.to_parquet(parquet_path, index=False)
        excel_path = os.path.join(output_dir, output_filename + ".xlsx")
        df.to_excel(excel_path, index=False)
        csv_path = os.path.join(output_dir, output_filename + ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Injected invalid values {invalid_values} into {parquet_path}")
    except Exception as e:
        print(f"⚠️ Could not inject invalid values: {e}")
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="invalid_values", timestamp=timestamp)

def run_missing_row_scenario(row_indices, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file, drop specified rows, upload, and run full ETL validation.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/missing_row_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    # Ensure parquet output for sidecar (Excel/CSV) creation
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    # Generate base file
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats,
           "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # Drop specified rows
    try:
        df = pd.read_parquet(parquet_path)
        df = df.drop(index=row_indices, errors='ignore')
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Dropped rows {row_indices} in file: {parquet_path}")
    except Exception as e:
        print(f"⚠️ Could not drop rows {row_indices}: {e}")
    # create sidecars
    try:
        df.to_excel(os.path.join(output_dir, output_filename + ".xlsx"), index=False)
        df.to_csv(os.path.join(output_dir, output_filename + ".csv"), index=False)
    except Exception:
        pass
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="missing_row", timestamp=timestamp)

def run_extra_columns_scenario(extra_columns, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file with extra columns, upload, and run full ETL validation.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/extra_columns_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    # Ensure parquet output for sidecar (Excel/CSV) creation
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats,
           "--output-dir", output_dir, "--output", output_filename,
           "--extra-columns", *extra_columns]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # create sidecars
    try:
        df = pd.read_parquet(parquet_path)
        df.to_excel(os.path.join(output_dir, output_filename + ".xlsx"), index=False)
        df.to_csv(os.path.join(output_dir, output_filename + ".csv"), index=False)
    except Exception:
        pass
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="extra_columns", timestamp=timestamp)

def run_min_max_limits_scenario(column_limits, rows=50, formats=["csv"], seed=None, extra_args=None, timestamp=None):
    """
    Generate test file, set columns to min/max and out-of-bounds values, upload, and run full ETL validation.
    column_limits: dict of {column: (min, max)}
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./test_output/min_max_limits_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}"
    # Ensure parquet output for sidecar (Excel/CSV) creation
    if "parquet" not in formats:
        formats = ["parquet"] + formats
    cmd = ["python", "newaugsver_clean.py", "--rows", str(rows), "--formats", *formats,
           "--output-dir", output_dir, "--output", output_filename]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if extra_args:
        cmd += extra_args
    print(f"🚀 Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    parquet_path = os.path.join(output_dir, output_filename + ".parquet")
    # Modify columns to min/max and out-of-bounds values
    try:
        df = pd.read_parquet(parquet_path)
        # Field constraints for string length, enums, etc.
        field_constraints = {
            'RecordOperation': {'enums': ['A', 'D']},
            'OrganizationCode': {'enums': ['M', 'D', 'P', 'R']},
            'ProfitNonprofit': {'enums': ['P', 'NP']},
            'OrganizationNPI': {'min_length': 10, 'max_length': 10, 'numeric': True},
            'PaymentMode': {'enums': ['EFT', 'CHK']},
            'RoutingTransitNumber': {'min_length': 9, 'max_length': 9, 'numeric': True},
            'AccountNumber': {'min_length': 6, 'max_length': 12, 'numeric': True},
            'AccountType': {'enums': ['CHKING', 'SAVING']},
            'OrganizationIdentifier': {'min_length': 3, 'max_length': 12},
            'PayeeID': {'min_length': 2, 'max_length': 7},
            'OrganizationName': {'min_length': 1, 'max_length': 40},
            'OrganizationLegalName': {'min_length': 1, 'max_length': 40},            'OrganizationTINType': {'enums': ['EIN', 'SSN']},
            'OrganizationTIN': {'min_length': 9, 'max_length': 9, 'numeric': True},
            'AddressCode': {'enums': ['COR', 'PMT']},
            'AddressLine1': {'min_length': 1, 'max_length': 40},
            'AddressLine2': {'min_length': 0, 'max_length': 40},
            'CityName': {'min_length': 1, 'max_length': 40},
            'State': {'min_length': 2, 'max_length': 2},
            'PostalCode': {'min_length': 5, 'max_length': 9},
            'ContactCode': {'enums': ['PRIM', 'SEC']},
            'ContactFirstName': {'min_length': 1, 'max_length': 40},
            'ContactLastName': {'min_length': 1, 'max_length': 40},
            'ContactTitle': {'min_length': 1, 'max_length': 23},
            'ContactPhone': {'min_length': 10, 'max_length': 25},
            'ContactFax': {'min_length': 10, 'max_length': 25},
            'ContactOtherPhone': {'min_length': 10, 'max_length': 25},
            'ContactEmail': {'min_length': 3, 'max_length': 99},
        }
        for col, (min_val, max_val) in column_limits.items():
            if col in df.columns:
                constraints = field_constraints.get(col, {})
                # Set first row to min, second to max
                if len(df) > 0:
                    df.at[0, col] = min_val
                if len(df) > 1:
                    df.at[1, col] = max_val
                # Below min
                if len(df) > 2:
                    if 'numeric' in constraints or (isinstance(min_val, (int, float)) and str(min_val).isdigit()):
                        try:
                            below_min = float(min_val) - 1
                        except Exception:
                            below_min = -999999
                        df.at[2, col] = below_min
                    elif 'min_length' in constraints:
                        below_min = 'X' * max(0, constraints['min_length'] - 1)
                        df.at[2, col] = below_min
                    elif 'enums' in constraints:
                        df.at[2, col] = 'INVALID_ENUM'
                    else:
                        df.at[2, col] = 'X'
                # Above max
                if len(df) > 3:
                    if 'numeric' in constraints or (isinstance(max_val, (int, float)) and str(max_val).isdigit()):
                        try:
                            above_max = float(max_val) + 1
                        except Exception:
                            above_max = 999999999
                        df.at[3, col] = above_max
                    elif 'max_length' in constraints:
                        above_max = 'A' * (constraints['max_length'] + 5)
                        df.at[3, col] = above_max
                    elif 'enums' in constraints:
                        df.at[3, col] = 'INVALID_ENUM'
                    else:
                        df.at[3, col] = 'TOO_LONG_VALUE'
        # Save updated files
        df.to_parquet(parquet_path, index=False)
        excel_path = os.path.join(output_dir, output_filename + ".xlsx")
        df.to_excel(excel_path, index=False)
        csv_path = os.path.join(output_dir, output_filename + ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Set min/max/out-of-bounds values for {list(column_limits.keys())} in {parquet_path}")
    except Exception as e:
        print(f"⚠️ Could not set min/max values: {e}")
    upload_to_s3(parquet_path)
    return run_full_etl_pipeline_with_existing_file(parquet_path, scenario_name="min_max_limits", timestamp=timestamp)

# Lambdas for Organization rules
ORG_IDENTIFIER_LAMBDA = {
    "D": "DISP",
    "P": "PC",
    "M": "MFR",
    "R": "RCPT"  # Add 'R' with its prefix
}

PAYEE_ID_LAMBDA = {
    "D": "D",
    "P": "P",
    "M": "M",
    "R": "R"  # Add 'R' with its prefix
}

def process_row(row):
    """
    Ensures OrganizationIdentifier and PayeeID share the same integer part.
    Always sets OrganizationIdentifier and PayeeID to the correct prefix + number.
    """
    code = row.get("OrganizationCode")
    payee_id = row.get("PayeeID")
    num = None

    # Extract the integer part from PayeeID (e.g., D149, DISP149, PC123)
    if payee_id:
        match = re.search(r"(\d+)$", str(payee_id))
        if match:
            num = match.group(1)

    if not num:
        num = str(random.randint(100, 999))

    # Always set PayeeID using the correct prefix and number
    if code in PAYEE_ID_LAMBDA and num:
        row["PayeeID"] = f"{PAYEE_ID_LAMBDA[code]}{num}"

    # Always set OrganizationIdentifier using the correct prefix and number
    if code in ORG_IDENTIFIER_LAMBDA and num:
        row["OrganizationIdentifier"] = f"{ORG_IDENTIFIER_LAMBDA[code]}{num}"

    return row

def clear_unnecessary_columns(df):
    """
    Clears all columns except specific ones for rows with OrganizationCode 'R'.
    """
    columns_to_keep = ["OrganizationCode", "PayeeID", "OrganizationIdentifier", "OrganizationName", "OrganizationLegalName"]
    for idx, row in df.iterrows():
        if row["OrganizationCode"] == "R":
            for col in df.columns:
                if col not in columns_to_keep:
                    df.at[idx, col] = None
    return df

def validate_orgid_payeeid(df):
    """
    Validates that OrganizationIdentifier and PayeeID share the same numeric part and correct prefix.
    Additionally, ensures PayeeID equals OrganizationIdentifier for OrgCode 'D' and 'P'.
    """
    for idx, row in df.iterrows():
        code = row["OrganizationCode"]
        payee_id = str(row["PayeeID"])
        org_id = str(row["OrganizationIdentifier"])

        # Validation for numeric part and prefix
        match = re.search(r"(\d+)$", payee_id)
        if not match:
            raise AssertionError(f"Row {idx}: PayeeID '{payee_id}' does not contain a number.")
        num = match.group(1)
        expected_org_id = ORG_IDENTIFIER_LAMBDA[code](num)
        assert org_id == expected_org_id, f"Row {idx}: OrganizationIdentifier '{org_id}' does not match expected '{expected_org_id}'"

        # Additional validation for OrgCode 'D' and 'P'
        if code in ["D", "P"]:
            assert payee_id == org_id, f"Row {idx}: PayeeID '{payee_id}' does not equal OrganizationIdentifier '{org_id}' for OrgCode '{code}'"

    print("✅ OrganizationIdentifier and PayeeID relationship validated.")

# --------------------
# Main entry point
# --------------------
def main():
    parser = argparse.ArgumentParser(description="ETL Validator Pipeline")
    parser.add_argument("--scenario", type=str, choices=['happy', 'valid', 'invalid'], help='Scenario to run (happy/valid/invalid)')
    parser.add_argument("--missing-column", type=str, help="Column to remove for missing column scenario")
    parser.add_argument("--missing-columns", nargs="+", help="Columns to remove for missing columns scenario")
    parser.add_argument("--rename-column", type=str, help="Rename a single column OldName:NewName")
    parser.add_argument("--rename-columns", nargs="+", help="Rename multiple columns Old1:New1 Old2:New2 ...")
    parser.add_argument("--row", type=int, help="Alias for --rows")
    parser.add_argument("--duplicate-row", type=int, nargs="?", const=0, help="Row index to duplicate")
    parser.add_argument("--invalid-values", nargs="+", help="Inject invalid values: Column:Value [Column2:Value2 ...]")
    parser.add_argument("--extra-columns", nargs="+", help="Add extra columns with random values")
    parser.add_argument("--invalid-extension", type=str, help="Save file with an invalid extension (e.g., txt)")
    parser.add_argument("--invalid-mfr-ein-ssn", nargs="?", const="SSN", default=None, help="Inject invalid EIN/SSN for Manufacturer rows (OrgCode M). Default is SSN.")
    parser.add_argument("--invalid-tin-type", type=str, help="Inject invalid OrganizationTINType value (e.g. Q)")
    parser.add_argument("--rows", type=int, default=50, help="Number of rows to generate")
    parser.add_argument("--formats", nargs="+", default=["csv"], help="Output formats (csv, parquet, xlsx, json)")
    parser.add_argument("--seed", type=int, help="Random seed")
    parser.add_argument("--run-all-scenarios", action="store_true", help="Run all major scenarios in sequence with seed 246")
    parser.add_argument("--drop-rows", nargs="+", type=int, help="Indices of rows to drop for missing row scenario")
    parser.add_argument("--run-all-row-validation", action="store_true", help="Run all row validation scenarios in sequence")
    parser.add_argument("--dev2", action="store_true", help="Use Dev2 environment (bucket2/glue2)")
    # add CLI flag for duplicate PayeeID scenario
    parser.add_argument("--duplicate-payee-id", action="store_true", help="Duplicate a PayeeID across two rows")
    parser.add_argument("--min-max-limits", nargs='+', help="Test min/max limits: Column:Min:Max [Column2:Min:Max ...]")
    # Add CLI flag for min/max all columns
    parser.add_argument("--min-max-all-columns", action="store_true", help="Test min/max and out-of-bounds for all columns at once")
    args, extra = parser.parse_known_args()

    # Switch environment if Dev2 root flag is set
    if args.dev2:
        global BUCKET, GLUE_JOB_NAME, ENV_SUFFIX
        BUCKET = BUCKET_2
        GLUE_JOB_NAME = GLUE_JOB_NAME_2
        ENV_SUFFIX = "dev2"

    # Run all scenarios if requested
    if args.run_all_scenarios:
        seed = 246
        scenarios = [
            # Happy path
            (lambda: run_test_scenario("valid", seed=seed)),            # Missing column: OrganizationTIN
            (lambda: run_missing_column_scenario("OrganizationTIN")),
            # Missing columns: OrganizationTIN, AccountNumber
            (lambda: run_missing_column_scenario("OrganizationTIN,AccountNumber")),
            # Invalid value: OrganizationTIN (simulate by injecting invalid value)
            (lambda: run_invalid_values_scenario(["OrganizationTIN:INVALIDTIN"], seed=seed)),
            # Extra column: ExtraCol1
            (lambda: run_extra_columns_scenario(["ExtraCol1"], seed=seed)),
            # Invalid extension: txt
            (lambda: run_invalid_extension_scenario("txt", seed=seed)),
        ]
        for scenario in scenarios:
            scenario()
        print("\n✅ All major scenarios completed with seed 246.")
        return
    # If scenario is provided, run it directly (no prompt)
    if args.scenario in ['happy', 'valid']:
        run_test_scenario("valid", seed=args.seed, rows=args.rows)
        return
    elif args.scenario == 'invalid':
        run_test_scenario("invalid", seed=args.seed, rows=args.rows)
        return
    # Normalize rows alias
    if getattr(args, 'row', None) is not None and (not args.rows or args.rows == 50):
        # If user specified --row and didn't override --rows explicitly, use it
        args.rows = args.row

    # Composite: rename + invalid values + optional missing columns
    if (args.rename_column or args.rename_columns or args.invalid_values) and (args.invalid_values or args.missing_columns or args.missing_column):
        # Determine if we need full composite (presence of at least two of the three groups)
        groups_present = sum([
            1 if (args.rename_column or args.rename_columns) else 0,
            1 if (args.invalid_values) else 0,
            1 if (args.missing_columns or args.missing_column) else 0
        ])
        if groups_present >= 2:
            specs = []
            if args.rename_column:
                specs.append(args.rename_column)
            if args.rename_columns:
                specs.extend(args.rename_columns)
            drop_cols = []
            if args.missing_column:
                drop_cols.append(args.missing_column)
            if args.missing_columns:
                drop_cols.extend(args.missing_columns)
            run_composite_transform_scenario(
                rename_specs=specs or None,
                invalid_values=args.invalid_values,
                drop_columns=drop_cols or None,
                rows=args.rows,
                formats=args.formats,
                seed=args.seed,
                extra_args=extra
            )
            return

    if args.missing_column:
        run_missing_column_scenario(args.missing_column, rows=args.rows)
        return
    elif args.missing_columns:
        run_missing_column_scenario(",".join(args.missing_columns), rows=args.rows)
        return
    elif args.rename_column or args.rename_columns:
        specs = []
        if args.rename_column:
            specs.append(args.rename_column)
        if args.rename_columns:
            specs.extend(args.rename_columns)
        run_rename_column_scenario(specs, rows=args.rows)
        return
    elif args.duplicate_row is not None:
        run_duplicate_row_scenario(args.duplicate_row, rows=args.rows)
        return
    elif args.invalid_values:
        run_invalid_values_scenario(
            invalid_values=args.invalid_values,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")
        )
        return
    elif args.extra_columns:
        run_extra_columns_scenario(
            extra_columns=args.extra_columns,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")
        )
        return
    elif args.invalid_extension:
        run_invalid_extension_scenario(
            extension=args.invalid_extension,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")
        )
        return
    elif args.invalid_mfr_ein_ssn is not None:
        run_invalid_mfr_ein_ssn_scenario(
            args.invalid_mfr_ein_ssn,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")
        )
        return
    elif args.invalid_tin_type:
        # Inject bad values into OrganizationTINType column
        run_invalid_values_scenario(
            invalid_values=[f"OrganizationTINType:{args.invalid_tin_type}"],
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")
        )
        return
    elif args.drop_rows:
        run_missing_row_scenario(row_indices=args.drop_rows)
        return
    elif args.run_all_row_validation:
        seed = args.seed or 246
        scenarios = [
            # 1. Null/Empty mandatory field
            lambda: run_invalid_values_scenario(["OrganizationTIN:"], rows=args.rows, formats=args.formats, seed=seed),
            # 2. Too short field
            lambda: run_invalid_values_scenario(["AccountNumber:123"], rows=args.rows, formats=args.formats, seed=seed),
            # 3. Too long field (numeric string exceeding max length)
            lambda: run_invalid_values_scenario(["AccountNumber:12345678901234567890"], rows=args.rows, formats=args.formats, seed=seed),
            # 4. Invalid enum value
            lambda: run_invalid_values_scenario(["OrganizationCode:Z"], rows=args.rows, formats=args.formats, seed=seed),
            # 5. Invalid format
            # 5. Invalid format
            lambda: run_invalid_values_scenario(["ContactEmail:not-an-email"], rows=args.rows, formats=args.formats, seed=seed),
            # 6. Numeric range low
            lambda: run_invalid_values_scenario(["SomeNumericField:-1"], rows=args.rows, formats=args.formats, seed=seed),
            # 7. Numeric range high
            lambda: run_invalid_values_scenario(["SomeNumericField:1000000000"], rows=args.rows, formats=args.formats, seed=seed),
            # 8. Invalid date format
            lambda: run_invalid_values_scenario(["TransactionDate:2025/13/01"], rows=args.rows, formats=args.formats, seed=seed),
            # 9. Date out-of-range past
            lambda: run_invalid_values_scenario(["TransactionDate:1800-01-01"], rows=args.rows, formats=args.formats, seed=seed),
            # 10. Date out-of-range future
            lambda: run_invalid_values_scenario(["TransactionDate:2050-01-01"], rows=args.rows, formats=args.formats, seed=seed),
            # 11. Cross-field mismatch
            lambda: run_invalid_values_scenario(["OrganizationIdentifier:BADID"], rows=args.rows, formats=args.formats, seed=seed),
            # 12. Invalid TIN type for Manufacturer rows
            lambda: run_invalid_values_scenario(["OrganizationTINType:Q"], rows=args.rows, formats=args.formats, seed=seed),
            # 13. Duplicate row (index 0)
            lambda: run_duplicate_row_scenario(row_index=0, timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")),
            # 14. Duplicate row (index 1)
            lambda: run_duplicate_row_scenario(row_index=1, timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")),
            # 15. Missing row (drop first row)
            lambda: run_missing_row_scenario(row_indices=[0], timestamp=datetime.now().strftime("%Y%m%d.%H%M%S")),
            # 16. Extra column
            lambda: run_extra_columns_scenario(extra_columns=["ExtraCol1"], rows=args.rows, formats=args.formats, seed=seed),
            # 17. Missing column
            lambda: run_missing_column_scenario("OrganizationTIN"),
            # 18. Missing multiple mandatory
            lambda: run_invalid_values_scenario(["OrganizationTIN:", "ContactEmail:"], rows=args.rows, formats=args.formats, seed=seed),
            # 19. Invalid file extension (.txt upload)
            lambda: run_invalid_extension_scenario("txt", rows=args.rows, formats=args.formats, seed=seed),
            # 20. Missing RecordOperation column entirely
            lambda: run_missing_column_scenario("RecordOperation"),
            # 21. Empty RecordOperation (set to '')
            lambda: run_invalid_values_scenario(["RecordOperation:"], rows=args.rows, formats=args.formats, seed=seed),
            # 22. Invalid ProfitNonprofit code
            lambda: run_invalid_values_scenario(["ProfitNonprofit:X"], rows=args.rows, formats=args.formats, seed=seed),
            # 23. Invalid OrganizationNPI format (letters)
            lambda: run_invalid_values_scenario(["OrganizationNPI:ABC1234567"], rows=args.rows, formats=args.formats, seed=seed),
            # 24. Invalid PaymentMode
            lambda: run_invalid_values_scenario(["PaymentMode:XYZ"], rows=args.rows, formats=args.formats, seed=seed),
            # 25. Routing Transit Number length (8 digits)
            lambda: run_invalid_values_scenario(["RoutingTransitNumber:12345678"], rows=args.rows, formats=args.formats, seed=seed),
            # 26. Account number too long (>17 chars)
            lambda: run_invalid_values_scenario(["AccountNumber:1234567890123456789"], rows=args.rows, formats=args.formats, seed=seed),
            # 27. Invalid AccountType
            lambda: run_invalid_values_scenario(["AccountType:CHECK"], rows=args.rows, formats=args.formats, seed=seed),
            # 28. Invalid State code
            lambda: run_invalid_values_scenario(["State:ZZ"], rows=args.rows, formats=args.formats, seed=seed),
            # 29. Invalid PostalCode
            lambda: run_invalid_values_scenario(["PostalCode:ABCDE"], rows=args.rows, formats=args.formats, seed=seed),
            # 30. Invalid ContactCode
            lambda: run_invalid_values_scenario(["ContactCode:XXX"], rows=args.rows, formats=args.formats, seed=seed),
            # 31. Over-length contact fields (first name, last name, title, phones)
            lambda: run_invalid_values_scenario([
                "ContactFirstName:ABCDEFGHIJKLMNOPQRSTU",
                "ContactLastName:ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                "ContactTitle:ABCDEFGHIJKLMNOPQRSTUVW",
                "ContactPhone:123ABC4567",
                "ContactOtherPhone:123-456-7890!"
            ], rows=args.rows, formats=args.formats, seed=seed),
            # 32. Invalid OrganizationIdentifier prefix mismatch
            lambda: run_invalid_values_scenario(["OrganizationIdentifier:BADID"], rows=args.rows, formats=args.formats, seed=seed),
        ]
        # execute row validation scenarios
        for scenario in scenarios:
            scenario()
        return

    # If no scenario or scenario-related args, default to valid scenario
    # Ensure we honor --rows and --seed provided on the CLI.
    run_test_scenario("valid", seed=args.seed, rows=args.rows)
    return

if __name__ == "__main__":
    main()
