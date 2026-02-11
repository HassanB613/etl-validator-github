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
    
    @allure.story("Valid Data Processing")
    @allure.title("Test: Valid bank file - full ETL pipeline")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_valid_scenario(self):
        """
        Test valid bank file with 25 rows processes correctly through full ETL pipeline.
        
        Steps:
        1. Generate valid parquet file with 25 rows
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        """
        with allure.step("Running full ETL pipeline for VALID scenario"):
            step_status, overall_status = run_test_scenario("valid", rows=25)
            
            # Attach step results to Allure report
            for step, status in step_status.items():
                allure.attach(
                    f"{step}: {status}",
                    name=step,
                    attachment_type=allure.attachment_type.TEXT
                )
            
            # Check overall result
            # overall_status: 1 = Passed, 5 = Failed
            assert overall_status == 1, f"Valid scenario failed. Step statuses: {step_status}"


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
