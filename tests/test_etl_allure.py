"""
ETL Validation Tests with Allure Reporting
Runs real ETL pipeline tests against Dev2 environment.
"""
import sys
import os
import subprocess
import pytest
import allure

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Import the pipeline module directly
from DM_bankfile_validate_pipeline import run_test_scenario


@allure.epic("ETL Validation Pipeline")
@allure.feature("Bank File Processing - Dev2")
class TestETLValidation:
    
    @allure.story("Invalid Data Processing")
    @allure.title("Test: Invalid bank file - error handling with DB validation")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_invalid_scenario(self):
        """
        Test invalid bank file with 25 rows triggers error handling and DB validation.
        
        Steps:
        1. Generate invalid parquet file (blanked OrganizationTIN and ContactEmail)
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify error file created in error folder
        7. Verify error file exists in error folder
        8. Database validation: Compare DB error count with CSV row count
        """
        with allure.step("Running full ETL pipeline for INVALID scenario"):
            step_status, overall_status, archive_path = run_test_scenario("invalid", rows=25)
            
            # Attach step results to Allure report
            for step, status in step_status.items():
                allure.attach(
                    f"{step}: {status}",
                    name=step,
                    attachment_type=allure.attachment_type.TEXT
                )
            
            # Attach archive file info
            allure.attach(
                archive_path,
                name="Archive File Location",
                attachment_type=allure.attachment_type.TEXT
            )
            
            # Check overall result
            # overall_status: 1 = Passed, 5 = Failed
            assert overall_status == 1, f"Invalid scenario failed. Step statuses: {step_status}"
    
    @allure.story("Valid Data Processing")
    @allure.title("Test: Valid bank file - successful processing")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_valid_scenario(self):
        """
        Test valid bank file with 25 rows is processed successfully.
        
        Steps:
        1. Generate valid parquet file (all fields populated correctly)
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify NO error file created in error folder
        7. Database validation: Verify 0 errors in DB
        """
        with allure.step("Running full ETL pipeline for VALID scenario"):
            step_status, overall_status, archive_path = run_test_scenario("valid", rows=25)
            
            # Attach step results to Allure report
            for step, status in step_status.items():
                allure.attach(
                    f"{step}: {status}",
                    name=step,
                    attachment_type=allure.attachment_type.TEXT
                )
            
            # Attach archive file info
            allure.attach(
                archive_path,
                name="Archive File Location",
                attachment_type=allure.attachment_type.TEXT
            )
            
            # Check overall result
            # overall_status: 1 = Passed, 5 = Failed
            assert overall_status == 1, f"Valid scenario failed. Step statuses: {step_status}"
    
    @allure.story("Valid Data Processing")
    @allure.title("Test: Valid bank file - 25 rows successful processing (second run)")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_valid_scenario_2(self):
        """
        Test valid bank file with 25 rows is processed successfully (second validation run).
        
        Steps:
        1. Generate valid parquet file (all fields populated correctly)
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify NO error file created in error folder
        7. Database validation: Verify 0 errors in DB
        """
        with allure.step("Running full ETL pipeline for VALID scenario (second run)"):
            step_status, overall_status, archive_path = run_test_scenario("valid", rows=25)
            
            # Attach step results to Allure report
            for step, status in step_status.items():
                allure.attach(
                    f"{step}: {status}",
                    name=step,
                    attachment_type=allure.attachment_type.TEXT
                )
            
            # Attach archive file info
            allure.attach(
                archive_path,
                name="Archive File Location",
                attachment_type=allure.attachment_type.TEXT
            )
            
            # Check overall result
            # overall_status: 1 = Passed, 5 = Failed
            assert overall_status == 1, f"Valid scenario (second run) failed. Step statuses: {step_status}"


@allure.epic("ETL Validation Pipeline")
@allure.feature("SQL Server Connectivity")
class TestSQLConnectivity:
    
    @allure.story("Database Connection")
    @allure.title("Test SQL Server Connection")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_sql_connection(self):
        """Test SQL Server database connectivity."""
        base_dir = os.path.dirname(os.path.dirname(__file__))
        sql_test_path = os.path.join(base_dir, "tests", "run_sql_test.py")
        
        with allure.step("Execute SQL connection test"):
            result = subprocess.run([sys.executable, sql_test_path], 
                                   capture_output=True, text=True)
            allure.attach(result.stdout, name="SQL Test Output", attachment_type=allure.attachment_type.TEXT)
            if result.returncode != 0:
                allure.attach(result.stderr, name="SQL Test Error", attachment_type=allure.attachment_type.TEXT)
            assert result.returncode == 0, "SQL connection test failed"
