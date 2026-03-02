import sys
import os
import subprocess
import pytest
import allure

"""
Test Run 1: Special Characters in RecordOperation Column
Injects special characters ($\) into the RecordOperation column to test ETL validation.
"""

@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test RecordOperation Column with Invalid Special Characters')
class TestRecordOperationValidation:
    
    @allure.description('Tests ETL pipeline validation by injecting special character ($) into RecordOperation column')
    @allure.severity(allure.severity_level.CRITICAL)
    def test_recordoperation_invalid_special_char(self):
        """Test that pipeline rejects invalid special characters in RecordOperation column"""
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        # Run the pipeline with special characters injected into RecordOperation column
        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values", "RecordOperation:$",
            "--dev2",
            "--rows", "25"
        ]
        
        with allure.step("Inject special character ($) into RecordOperation column"):
            print("=" * 60)
            print("TEST: Special Characters in RecordOperation Column")
            print("=" * 60)
            print(f"Injecting: $ into RecordOperation column")
            print(f"Command: {' '.join(pipe_command)}")
            print("=" * 60)
        
        with allure.step("Run ETL pipeline"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
        
        with allure.step("Verify pipeline detected invalid data"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)
            
            # Pipeline should fail due to invalid data
            assert pipe_result.returncode == 0, f"Pipeline failed with return code {pipe_result.returncode}"
