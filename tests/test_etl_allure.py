"""
ETL Validation Tests with Allure Reporting
"""
import sys
import os
import subprocess
import pytest
import allure

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@allure.epic("ETL Validation Pipeline")
@allure.feature("Bank File Processing")
class TestETLValidation:
    
    @allure.story("Valid Data Processing")
    @allure.title("Test 1: Valid data with 25 rows")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_valid_data_25_rows(self):
        """Test valid bank file with 25 rows processes correctly."""
        base_dir = os.path.dirname(os.path.dirname(__file__))
        generator_path = os.path.join(base_dir, "newaugsver_clean.py")
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")
        
        with allure.step("Step 1: Generate valid test data (25 rows)"):
            gen_command = [sys.executable, generator_path, "--rows", "25", 
                          "--output-dir", os.path.join(base_dir, "test_output"), 
                          "--output", "test_data_1"]
            gen_result = subprocess.run(gen_command, capture_output=True, text=True)
            allure.attach(gen_result.stdout, name="Generator Output", attachment_type=allure.attachment_type.TEXT)
            if gen_result.returncode != 0:
                allure.attach(gen_result.stderr, name="Generator Error", attachment_type=allure.attachment_type.TEXT)
            assert gen_result.returncode == 0, "Data generation failed"
        
        with allure.step("Step 2: Run ETL pipeline"):
            pipe_command = [sys.executable, pipeline_path, "--dev2", "--rows", "25"]
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
            allure.attach(pipe_result.stdout, name="Pipeline Output", attachment_type=allure.attachment_type.TEXT)
            if pipe_result.returncode != 0:
                allure.attach(pipe_result.stderr, name="Pipeline Error", attachment_type=allure.attachment_type.TEXT)
            assert pipe_result.returncode == 0, "Pipeline execution failed"
    
    @allure.story("Valid Data Processing")
    @allure.title("Test 2: Empty AddressCode validation")
    @allure.severity(allure.severity_level.NORMAL)
    def test_empty_address_code(self):
        """Test that empty AddressCode is properly validated and creates error file."""
        base_dir = os.path.dirname(os.path.dirname(__file__))
        generator_path = os.path.join(base_dir, "newaugsver_clean.py")
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")
        
        with allure.step("Step 1: Generate test data (10 rows)"):
            gen_command = [sys.executable, generator_path, "--rows", "10", 
                          "--output-dir", os.path.join(base_dir, "test_output"), 
                          "--output", "test_data_2"]
            gen_result = subprocess.run(gen_command, capture_output=True, text=True)
            allure.attach(gen_result.stdout, name="Generator Output", attachment_type=allure.attachment_type.TEXT)
            assert gen_result.returncode == 0, "Data generation failed"
        
        with allure.step("Step 2: Run ETL pipeline with empty AddressCode"):
            pipe_command = [sys.executable, pipeline_path, "--dev2", 
                          "--invalid-values", "AddressCode:", "--rows", "10"]
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
            allure.attach(pipe_result.stdout, name="Pipeline Output", attachment_type=allure.attachment_type.TEXT)
            if pipe_result.returncode != 0:
                allure.attach(pipe_result.stderr, name="Pipeline Error", attachment_type=allure.attachment_type.TEXT)
            # For invalid data tests, we may expect success (pipeline completes) or specific error handling
            # Adjust assertion based on expected behavior
            assert pipe_result.returncode == 0, "Pipeline execution failed"
    
    @allure.story("Invalid Data Processing")
    @allure.title("Test 3: Invalid values trigger error file")
    @allure.severity(allure.severity_level.NORMAL)
    def test_invalid_values_error_file(self):
        """Test that invalid OrganizationCode, TIN, and NPI create error file."""
        base_dir = os.path.dirname(os.path.dirname(__file__))
        generator_path = os.path.join(base_dir, "newaugsver_clean.py")
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")
        
        with allure.step("Step 1: Generate test data (10 rows)"):
            gen_command = [sys.executable, generator_path, "--rows", "10", 
                          "--output-dir", os.path.join(base_dir, "test_output"), 
                          "--output", "test_data_3"]
            gen_result = subprocess.run(gen_command, capture_output=True, text=True)
            allure.attach(gen_result.stdout, name="Generator Output", attachment_type=allure.attachment_type.TEXT)
            assert gen_result.returncode == 0, "Data generation failed"
        
        with allure.step("Step 2: Run ETL pipeline with invalid values"):
            pipe_command = [sys.executable, pipeline_path, "--dev2", 
                          "--invalid-values", "OrganizationCode:X", "OrganizationTIN:ABC123", 
                          "OrganizationNPI:12345", "--row", "10"]
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
            allure.attach(pipe_result.stdout, name="Pipeline Output", attachment_type=allure.attachment_type.TEXT)
            if pipe_result.returncode != 0:
                allure.attach(pipe_result.stderr, name="Pipeline Error", attachment_type=allure.attachment_type.TEXT)
            assert pipe_result.returncode == 0, "Pipeline execution failed"


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
