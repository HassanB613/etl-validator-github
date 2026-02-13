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
import pyodbc  # Add this for SQL Server database validation

# --------------------
# Configuration
# --------------------
# Using Dev2 environment for testing
BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
BUCKET_DEV2 = "mtfpm-dev-s3-mtfdmstaging-us-east-1"  # Dev2 (backup)

GLUE_JOB_NAME = "load-bank-file-stg-dev2"
GLUE_JOB_NAME_DEV1 = "mtfpm-bankfile-validation-error-handling-dev"  # Dev1 (backup)

S3_PREFIX = "bankfile/ready"           # Update to the correct prefix
ARCHIVE_PREFIX = "bankfile/archive/2025/07"
ARCHIVE_PREFIX_2 = "bankfile/archive/2025/07"  # Dev2 archive prefix
ERROR_CSV_PREFIX = "bankfile/error/"
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")  # Global timestamp for filenames
ENV_SUFFIX = "dev2"

s3 = boto3.client("s3")
glue = boto3.client("glue", region_name="us-east-1")

# Load SQL Server configuration for database validation
sql_config = configparser.ConfigParser()
sql_config_paths = [
    "sqlUtils/sqlconfig.ini",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlUtils/sqlconfig.ini")
]
SQL_SERVER = ""
SQL_DATABASE = ""
SQL_USERNAME = ""
SQL_PASSWORD = ""
for sql_config_path in sql_config_paths:
    if os.path.exists(sql_config_path):
        sql_config.read(sql_config_path)
        if "Credentials" in sql_config:
            SQL_SERVER = sql_config["Credentials"].get("Server", "")
            SQL_DATABASE = sql_config["Credentials"].get("Database", "")
            SQL_USERNAME = sql_config["Credentials"].get("UserName", "")
            SQL_PASSWORD = sql_config["Credentials"].get("Password", "")
            print(f"✅ SQL Server config loaded: {SQL_SERVER}/{SQL_DATABASE}")
        break
else:
    print("⚠️ sqlconfig.ini not found. Database validation will be skipped.")

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
    TESTRAIL_TEST_ID = int(config["TestRail"]["test_id"])
else:
    # Default values if config not found
    TESTRAIL_URL = ""
    TESTRAIL_USERNAME = ""
    TESTRAIL_API_KEY = ""
    TESTRAIL_TEST_ID = 0
    print("Warning: testrail_config.ini not found. TestRail reporting will be skipped.")

# --------------------
# TestRail Configuration
# --------------------
def get_allure_report_path():
    """
    Find the Allure report directory in Jenkins workspace.
    Returns the path if found, None otherwise.
    """
    # Common Allure report locations in Jenkins
    possible_paths = [
        os.path.join(os.environ.get("WORKSPACE", "."), "allure-report"),
        "./allure-report",
        "../allure-report",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "allure-report")
    ]
    
    for path in possible_paths:
        if os.path.exists(path) and os.path.isdir(path):
            print(f"📁 Found Allure report at: {path}")
            return path
    
    print("ℹ️ Allure report directory not found")
    return None

def zip_allure_report(allure_path):
    """
    Zip the Allure report folder for offline viewing.
    Returns the path to the zip file, or None if failed.
    Includes a VIEW_REPORT.bat file for easy viewing by non-technical users.
    """
    import shutil
    import zipfile
    
    if not allure_path or not os.path.exists(allure_path):
        return None
    
    try:
        # Create zip file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"allure_report_{timestamp}.zip"
        zip_path = os.path.join(os.path.dirname(allure_path), zip_filename)
        
        print(f"📦 Zipping Allure report to: {zip_path}")
        
        # Create a batch file for easy viewing (non-tech users just double-click this)
        view_bat_content = '''@echo off
echo ============================================
echo    Opening Allure Test Report
echo ============================================
echo.
echo Starting local web server...
echo (Keep this window open while viewing the report)
echo.
echo Report will open in your browser at: http://localhost:8000
echo.
echo To close: Press Ctrl+C or close this window
echo ============================================
start http://localhost:8000
python -m http.server 8000
pause
'''
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add all report files
            for root, dirs, files in os.walk(allure_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, allure_path)
                    zipf.write(file_path, arcname)
            
            # Add the VIEW_REPORT.bat file
            zipf.writestr("VIEW_REPORT.bat", view_bat_content)
            
            # Add a README for instructions
            readme_content = '''ALLURE TEST REPORT - HOW TO VIEW
=================================

EASY WAY (Windows):
1. Extract this entire zip folder
2. Double-click "VIEW_REPORT.bat"
3. Report opens in your browser automatically
4. Keep the black window open while viewing
5. Close the black window when done

MANUAL WAY:
1. Extract this zip folder
2. Open Command Prompt in this folder
3. Run: python -m http.server 8000
4. Open browser to: http://localhost:8000

NOTE: You need Python installed on your computer.
Download from: https://www.python.org/downloads/
'''
            zipf.writestr("README.txt", readme_content)
        
        zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"✅ Allure report zipped: {zip_filename} ({zip_size_mb:.2f} MB)")
        return zip_path
    except Exception as e:
        print(f"❌ Failed to zip Allure report: {e}")
        return None

def upload_attachment_to_testrail(result_id, file_path):
    """
    Upload a file attachment to a TestRail test result.
    :param result_id: The TestRail result ID (returned from add_result API)
    :param file_path: Path to the file to upload
    """
    if not os.path.exists(file_path):
        print(f"❌ Attachment file not found: {file_path}")
        return False
    
    url = f"{TESTRAIL_URL}index.php?/api/v2/add_attachment_to_result/{result_id}"
    
    try:
        with open(file_path, 'rb') as f:
            # TestRail expects multipart/form-data for file uploads
            files = {'attachment': (os.path.basename(file_path), f)}
            response = requests.post(
                url, 
                files=files, 
                auth=(TESTRAIL_USERNAME, TESTRAIL_API_KEY)
            )
        
        if response.status_code == 200:
            print(f"✅ Attachment uploaded to TestRail: {os.path.basename(file_path)}")
            return True
        else:
            print(f"❌ Failed to upload attachment: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error uploading attachment: {e}")
        return False

def report_to_testrail(test_id, status, comment):
    """
    Report test results to TestRail using test ID.
    Automatically attaches Allure report zip when running in Jenkins.
    :param test_id: TestRail test ID
    :param status: Test result status (1=Passed, 2=Blocked, 3=Untested, 4=Retest, 5=Failed)
    :param comment: Additional comments for the test result
    """
    if not TESTRAIL_URL or not TESTRAIL_API_KEY:
        print("⚠️ TestRail not configured. Skipping test result reporting.")
        return
    
    # Add Jenkins build info to comment if available
    build_url = os.environ.get("BUILD_URL")
    build_number = os.environ.get("BUILD_NUMBER")
    if build_url and build_number:
        comment = f"{comment}\n\n🔗 Jenkins Build #{build_number}: {build_url}"
    
    url = f"{TESTRAIL_URL}index.php?/api/v2/add_result/{test_id}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "status_id": status,
        "comment": comment
    }
    response = requests.post(url, json=payload, auth=(TESTRAIL_USERNAME, TESTRAIL_API_KEY))
    
    if response.status_code == 200:
        print(f"✅ Test result reported to TestRail for test {test_id}")
        result_data = response.json()
        result_id = result_data.get("id")
        
        # Try to attach Allure report if running in Jenkins
        if result_id and os.environ.get("BUILD_URL"):
            allure_path = get_allure_report_path()
            if allure_path:
                zip_path = zip_allure_report(allure_path)
                if zip_path:
                    upload_attachment_to_testrail(result_id, zip_path)
                    # Clean up zip file after upload
                    try:
                        os.remove(zip_path)
                        print(f"🗑️ Cleaned up temporary zip file")
                    except:
                        pass
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
# SQL Server Database Validation Functions
# --------------------
def get_sql_connection():
    """
    Create a connection to SQL Server using pyodbc.
    Returns connection object or None if connection fails.
    """
    if not SQL_SERVER or not SQL_DATABASE:
        print("⚠️ SQL Server not configured. Skipping database validation.")
        return None
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, timeout=30)
        print(f"✅ Connected to SQL Server: {SQL_DATABASE}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to SQL Server: {e}")
        return None

def get_ins_batch_id_from_job_id(glue_run_id):
    """
    Query JOB_CONTROL table to get BATCH_ID for a given Glue Job Run ID.
    The BATCH_ID value is then used as INS_BATCH_ID in PAYEE_ERROR_STG.
    Returns BATCH_ID or None if not found.
    """
    conn = get_sql_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        query = """
            SELECT BATCH_ID 
            FROM [MTFDM_STG].[JOB_CONTROL] 
            WHERE JOB_ID = ?
        """
        print(f"🔍 Looking up BATCH_ID for Glue Job: {glue_run_id}")
        cursor.execute(query, (glue_run_id,))
        row = cursor.fetchone()
        if row:
            batch_id = row[0]
            print(f"✅ Found BATCH_ID: {batch_id}")
            return batch_id
        else:
            print(f"❌ No JOB_CONTROL record found for JOB_ID: {glue_run_id}")
            return None
    except Exception as e:
        print(f"❌ Error querying JOB_CONTROL: {e}")
        return None
    finally:
        conn.close()

def get_error_count_from_db(ins_batch_id):
    """
    Query PAYEE_ERROR_STG table to count error rows for a given INS_BATCH_ID.
    The INS_BATCH_ID value comes from BATCH_ID in JOB_CONTROL table.
    Returns count or None if query fails.
    """
    conn = get_sql_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        query = """
            SELECT COUNT(*) AS error_count 
            FROM [MTFDM_STG].[PAYEE_ERROR_STG] 
            WHERE INS_BATCH_ID = ?
        """
        print(f"🔍 Counting errors in PAYEE_ERROR_STG for INS_BATCH_ID: {ins_batch_id}")
        cursor.execute(query, (ins_batch_id,))
        row = cursor.fetchone()
        if row:
            error_count = row[0]
            print(f"✅ Found {error_count} error rows in database")
            return error_count
        return 0
    except Exception as e:
        print(f"❌ Error querying PAYEE_ERROR_STG: {e}")
        return None
    finally:
        conn.close()

def download_latest_error_csv_from_s3(local_evidence_dir):
    """
    Download the most recent error CSV file from S3 error folder (by LastModified date).
    Returns (local_file_path, row_count) or (None, 0) if not found.
    """
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=ERROR_CSV_PREFIX)
        contents = result.get("Contents", [])
        
        # Filter to only CSV files and sort by LastModified descending
        csv_files = [obj for obj in contents if obj["Key"].endswith(".csv")]
        if not csv_files:
            print(f"❌ No CSV files found in s3://{BUCKET}/{ERROR_CSV_PREFIX}")
            return None, 0
        
        # Sort by LastModified to get the most recent file
        csv_files.sort(key=lambda x: x["LastModified"], reverse=True)
        latest_file = csv_files[0]
        
        print(f"📥 Latest error file: {latest_file['Key']} (Modified: {latest_file['LastModified']})")
        
        # Download the file
        os.makedirs(local_evidence_dir, exist_ok=True)
        local_path = os.path.join(local_evidence_dir, os.path.basename(latest_file["Key"]))
        s3.download_file(BUCKET, latest_file["Key"], local_path)
        print(f"✅ Downloaded latest error file to: {local_path}")
        
        # Count rows in CSV (excluding header)
        try:
            df = pd.read_csv(local_path)
            row_count = len(df)
            print(f"✅ Error CSV contains {row_count} data rows")
            return local_path, row_count
        except Exception as e:
            print(f"⚠️ Could not read CSV to count rows: {e}")
            return local_path, 0
            
    except Exception as e:
        print(f"❌ Failed to download latest error CSV: {e}")
        return None, 0

def validate_error_file_with_database(glue_run_id, local_evidence_dir):
    """
    Full database validation workflow for error file testing:
    1. Get BATCH_ID from JOB_CONTROL using Glue Run ID (JOB_ID column)
    2. Use that BATCH_ID to count error rows in PAYEE_ERROR_STG (INS_BATCH_ID column)
    3. Download latest error CSV from S3
    4. Compare row counts
    
    Returns (passed, details_dict) where passed is True if counts match.
    """
    print("\n>>> Step 8: Database Error File Validation")
    details = {
        "glue_run_id": glue_run_id,
        "ins_batch_id": None,
        "db_error_count": None,
        "csv_error_count": None,
        "csv_file": None,
        "match": False
    }
    
    if not glue_run_id:
        print("⚠️ No Glue Run ID provided. Skipping database validation.")
        return False, details
    
    # Step 1: Get INS_BATCH_ID from JOB_CONTROL
    ins_batch_id = get_ins_batch_id_from_job_id(glue_run_id)
    details["ins_batch_id"] = ins_batch_id
    if not ins_batch_id:
        print("❌ Could not retrieve INS_BATCH_ID. Database validation failed.")
        return False, details
    
    # Step 2: Count error rows in database
    db_error_count = get_error_count_from_db(ins_batch_id)
    details["db_error_count"] = db_error_count
    if db_error_count is None:
        print("❌ Could not count errors in database. Validation failed.")
        return False, details
    
    # Step 3: Download latest error CSV from S3
    csv_path, csv_error_count = download_latest_error_csv_from_s3(local_evidence_dir)
    details["csv_file"] = csv_path
    details["csv_error_count"] = csv_error_count
    
    if not csv_path:
        print("❌ Could not download error CSV. Validation failed.")
        return False, details
    
    # Step 4: Compare counts
    if db_error_count == csv_error_count:
        print(f"✅ Row counts MATCH: DB={db_error_count}, CSV={csv_error_count}")
        details["match"] = True
        return True, details
    else:
        print(f"❌ Row counts MISMATCH: DB={db_error_count}, CSV={csv_error_count}")
        details["match"] = False
        return False, details

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
def wait_for_ready_folder_empty_and_glue_idle(job_name, timeout=300):
    """
    Wait for the ready folder to be empty and no Glue job running before proceeding.
    This prevents uploading a new file while a previous test is still in progress.
    """
    print("🔍 Checking if ready folder is empty and Glue is idle before uploading...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        # Check if Glue job is running
        running_job = get_running_glue_job(job_name)
        if running_job:
            print(f"⏳ Glue job {running_job} is still running. Waiting...")
            time.sleep(15)
            continue
        
        # Check if ready folder has any files
        try:
            result = s3.list_objects_v2(Bucket=BUCKET, Prefix=S3_PREFIX + "/")
            files = [obj["Key"] for obj in result.get("Contents", []) if not obj["Key"].endswith("/")]
            if files:
                print(f"⏳ Ready folder still has files: {files}. Waiting for Glue to process...")
                time.sleep(15)
                continue
        except Exception as e:
            print(f"⚠️ Could not check ready folder: {e}")
        
        # Both conditions met - ready to proceed
        print("✅ Ready folder is empty and Glue is idle. Safe to upload new file.")
        return True
    
    print("⚠️ Timeout waiting for ready folder to clear. Proceeding anyway...")
    return False

def upload_to_s3(file_path):
    """
    Upload files to S3 with the new naming convention.
    Waits for any running Glue job to complete first.
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    # Wait for previous test to complete before uploading
    wait_for_ready_folder_empty_and_glue_idle(GLUE_JOB_NAME)

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
    
    Returns tuple: (success: bool, run_id: str or None)
    """
    run_id = None
    
    # Wait a moment for S3 trigger to potentially start the Glue job
    print("⏳ Waiting 15 seconds for S3 trigger to potentially start Glue job...")
    time.sleep(15)
    
    # Check multiple times for a running job (S3 trigger may have a delay)
    print("🔍 Checking if Glue job is already running...")
    for check_attempt in range(3):
        existing_run_id = get_running_glue_job(job_name)
        if existing_run_id:
            print(f"✅ Glue job is already running (RunId: {existing_run_id}). Monitoring existing run...")
            run_id = existing_run_id
            break
        if check_attempt < 2:
            print(f"⏳ No running job found yet, waiting 10s (check {check_attempt + 1}/3)...")
            time.sleep(10)
    
    if not run_id:
        print("🕒 No running Glue job found after checks. Starting new Glue job...")
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
                return False, None
    
    if not run_id:
        print("❌ Could not start or find Glue job.")
        return False, None

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
                return status == "SUCCEEDED", run_id
        except Exception as e:
            print(f"⚠️ Error checking job status: {e}")
        time.sleep(10)

    print("❌ Timeout waiting for Glue job to complete.")
    return False, run_id

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
        "Step 7": "Passed",
        "Step 8": "Skipped"  # Database validation (only for invalid/error scenarios)
    }
    file_path = None  # Initialize file_path
    glue_run_id = None  # Track Glue run ID for database validation
    try:
        print(f"\n>>> Running test for scenario: {file_type.upper()}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
        glue_job_success, glue_run_id = wait_for_glue_success(GLUE_JOB_NAME)
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
                # For valid scenario, check that NO error file exists for this specific timestamp
                expected_error_file = check_expected_error_file_exists(ERROR_CSV_PREFIX, timestamp)
                assert not expected_error_file, f"❌ Unexpected error file found for valid scenario: {expected_error_file}"
                print(f"✅ No error file found for this timestamp (as expected for valid scenario).")
            else:
                # For invalid scenario, skip S3 error file check - Step 8 DB validation handles this
                step_status["Step 7"] = "Skipped (invalid scenario - DB validation in Step 8)"
                print(f"ℹ️ Skipping S3 error file check for invalid scenario (using DB validation in Step 8)")
        except AssertionError as e:
            step_status["Step 7"] = f"Failed: {str(e)}"
            print(str(e))

        # Step 8: Database validation (only for invalid/error scenarios)
        if not is_valid and glue_run_id:
            test_output_dir = os.path.dirname(file_path) if file_path else "./test_output"
            evidence_dir = os.path.join(test_output_dir, "test evidence s3 ready folder")
            db_validation_passed, db_details = validate_error_file_with_database(glue_run_id, evidence_dir)
            if db_validation_passed:
                step_status["Step 8"] = f"Passed (DB={db_details['db_error_count']}, CSV={db_details['csv_error_count']})"
            else:
                step_status["Step 8"] = f"Failed: DB={db_details.get('db_error_count', 'N/A')}, CSV={db_details.get('csv_error_count', 'N/A')}"
        elif is_valid:
            step_status["Step 8"] = "Skipped (valid scenario - no errors expected)"

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
            
            # Download only the specific expected error file for this timestamp
            error_files = download_specific_error_file(ERROR_CSV_PREFIX, evidence_dir, timestamp)
            
            # Save S3 listings for reference
            save_s3_listing_to_file(ERROR_CSV_PREFIX, evidence_dir, "s3_error_listing_before_delete.txt")
            
            if not archive_downloaded and error_files == 0:
                if os.path.exists(evidence_dir) and not os.listdir(evidence_dir):
                    os.rmdir(evidence_dir)
                    print(f"🗑️ Removed empty evidence directory: {evidence_dir}")
        
        # --- Build archive file info for return ---
        archive_filename = f"mtfdm_{ENV_SUFFIX}_dmbankdata_{timestamp}.parquet"
        archive_s3_path = f"s3://{BUCKET}/bankfile/archive/{datetime.now().strftime('%Y')}/{datetime.now().strftime('%m')}/{archive_filename}"
        
        # --- Now report to TestRail ---
        detailed_comment = f"Scenario: {file_type}\n" + "\n".join([f"{step}: {status}" for step, status in step_status.items()])
        overall_status = 5 if any("Failed" in str(status) for status in step_status.values()) else 1
        if overall_status == 5:
            print("❌ Overall Test Result: Failed")
        else:
            print("✅ Overall Test Result: Passed")
        report_to_testrail(TESTRAIL_TEST_ID, overall_status, detailed_comment)
        return step_status, overall_status, archive_s3_path

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

def check_expected_error_file_exists(prefix, timestamp):
    """
    Check if the specific expected error file exists in S3.
    Expected filename: mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}.csv
    Returns the S3 key if found, None otherwise.
    """
    expected_filename = f"mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}.csv"
    expected_key = f"{prefix}{expected_filename}"
    print(f"🔍 Searching for expected error file: {expected_filename}")
    try:
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in result.get("Contents", []):
            if obj["Key"].endswith(expected_filename):
                print(f"✅ Found expected error file: {obj['Key']}")
                return obj["Key"]
        print(f"❌ Expected error file not found: {expected_filename}")
        return None
    except Exception as e:
        print(f"⚠️ Failed to check for error file: {str(e)}")
        print(f"ℹ️ Skipping error file check. Please configure AWS credentials if S3 access is required.")
        return None

def download_specific_error_file(s3_prefix, local_evidence_dir, timestamp):
    """
    Download only the specific expected error file (by exact filename) to the evidence directory.
    Expected filename: mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}.csv
    Returns 1 if file was downloaded, 0 otherwise.
    """
    try:
        error_file_key = check_expected_error_file_exists(s3_prefix, timestamp)
        if not error_file_key:
            print(f"ℹ️ Expected error file not found for timestamp '{timestamp}'")
            return 0
        os.makedirs(local_evidence_dir, exist_ok=True)
        local_path = os.path.join(local_evidence_dir, os.path.basename(error_file_key))
        print(f"⬇️ Downloading error file {error_file_key} to {local_path}")
        s3.download_file(BUCKET, error_file_key, local_path)
        print(f"✅ Successfully downloaded error file: {os.path.basename(error_file_key)}")
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
        "Step 7": "Pending",
        "Step 8": "Pending"  # Database validation for error file testing
    }
    file_type = scenario_name
    glue_run_id = None  # Track Glue run ID for database validation
    try:
        print(f"\n>>> Running full ETL pipeline for scenario: {scenario_name}")
        print(">>> Step 3: Validate S3 outputs before triggering Glue job")
        file_found = check_s3_file_exists_with_naming_convention(S3_PREFIX, timestamp)
        assert file_found, f"❌ File not found in S3: {file_path}"
        print(f"✅ File is present in S3.")
        step_status["Step 3"] = "Passed"

        print(">>> Step 4: Trigger and monitor Glue job")
        glue_job_success, glue_run_id = wait_for_glue_success(GLUE_JOB_NAME)
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
            # For these scenarios, check for the specific expected error file
            expected_error_file = check_expected_error_file_exists(ERROR_CSV_PREFIX, timestamp)
            assert expected_error_file, f"❌ Expected error file not found: mtfdm_{ENV_SUFFIX}_dmbankerrorfile_{timestamp}.csv"
            print(f"✅ Found expected error file: {expected_error_file}")
            step_status["Step 7"] = "Passed"
        except AssertionError as e:
            step_status["Step 7"] = f"Failed: {str(e)}"
            print(str(e))

        # Step 8: Database validation for error file testing
        if glue_run_id:
            test_output_dir = os.path.dirname(file_path) if file_path else "./test_output"
            evidence_dir = os.path.join(test_output_dir, "test evidence s3 ready folder")
            db_validation_passed, db_details = validate_error_file_with_database(glue_run_id, evidence_dir)
            if db_validation_passed:
                step_status["Step 8"] = f"Passed (DB={db_details['db_error_count']}, CSV={db_details['csv_error_count']})"
            else:
                step_status["Step 8"] = f"Failed: DB={db_details.get('db_error_count', 'N/A')}, CSV={db_details.get('csv_error_count', 'N/A')}"
        else:
            step_status["Step 8"] = "Skipped (no Glue run ID available)"

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
            
            # Download only the specific expected error file for this timestamp
            error_files = download_specific_error_file(ERROR_CSV_PREFIX, evidence_dir, timestamp)
            
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

    # Switch environment if Dev1 is requested (default is now Dev2)
    if args.dev2:
        # Already using Dev2 as default, this flag is now a no-op
        pass

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
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        )
        return
    elif args.extra_columns:
        run_extra_columns_scenario(
            extra_columns=args.extra_columns,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        )
        return
    elif args.invalid_extension:
        run_invalid_extension_scenario(
            extension=args.invalid_extension,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        )
        return
    elif args.invalid_mfr_ein_ssn is not None:
        run_invalid_mfr_ein_ssn_scenario(
            args.invalid_mfr_ein_ssn,
            rows=args.rows,
            formats=args.formats,
            seed=args.seed,
            extra_args=extra,
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
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
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
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
            lambda: run_duplicate_row_scenario(row_index=0, timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")),
            # 14. Duplicate row (index 1)
            lambda: run_duplicate_row_scenario(row_index=1, timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")),
            # 15. Missing row (drop first row)
            lambda: run_missing_row_scenario(row_indices=[0], timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")),
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
