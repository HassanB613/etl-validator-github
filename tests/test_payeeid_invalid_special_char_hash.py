import sys
import os
import subprocess
import pytest
import allure

"""
Test Run: Special Characters in PayeeID Column
Injects special characters (###) into the PayeeID column to test ETL validation.
"""

@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test PayeeID Column with Invalid Special Characters (###)')
class TestPayeeIDSpecialChar:
    
    @allure.description("""
    Test that pipeline rejects invalid special characters in PayeeID column.
    
    Steps:
    1. Generate invalid parquet file with special characters (###) in PayeeID
    2. Upload to S3 ready folder
    3. Trigger/monitor Glue job
    4. Verify file removed from ready folder
    5. Verify file moved to archive folder
    6. Verify error file created in error folder
    7. Database validation: Compare DB error count with CSV row count
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_payeeid_invalid_special_char_hash(self):
        """
        Test that pipeline rejects invalid special characters in PayeeID column.
        
        Steps:
        1. Generate invalid parquet file with special characters (###) in PayeeID
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify error file created in error folder
        7. Database validation: Compare DB error count with CSV row count
        """
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        # Run the pipeline with special characters (###) injected into PayeeID column
        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values", "PayeeID:###",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_payeeid_invalid_special_char_hash"
        ]
        
        with allure.step("Inject special characters (###) into PayeeID column"):
            print("=" * 60)
            print("TEST: Special Characters '###' in PayeeID Column")
            print("=" * 60)
            print(f"Injecting: ### into PayeeID column")
            print(f"Command: {' '.join(pipe_command)}")
            print("=" * 60)
        
        with allure.step("Run ETL pipeline"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True, encoding='utf-8')
        
        with allure.step("Verify pipeline detected invalid data"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)
            
            # Pipeline should complete without error
            assert pipe_result.returncode == 0, f"Pipeline crashed with return code {pipe_result.returncode}"
            
            # Verify validation detected errors (check for success indication)
            assert "Row counts MATCH" in pipe_result.stdout, f"Validation failed - output should contain 'Row counts MATCH', but got: {pipe_result.stdout[-500:]}"

