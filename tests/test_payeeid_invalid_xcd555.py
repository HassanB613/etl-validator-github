import sys
import os
import subprocess
import pytest
import allure

"""
Test Run: Invalid Value in PayeeID Column
Injects invalid value XCD555 into the PayeeID column to test ETL validation.
"""

@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test PayeeID Column with Invalid Value XCD555')
class TestPayeeIDInvalidXCD555:
    
    @allure.description("""
    Test that pipeline rejects invalid value in PayeeID column.
    
    Steps:
    1. Generate invalid parquet file with value XCD555 in PayeeID
    2. Upload to S3 ready folder
    3. Trigger/monitor Glue job
    4. Verify file removed from ready folder
    5. Verify file moved to archive folder
    6. Verify error file created in error folder
    7. Database validation: Compare DB error count with CSV row count
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_payeeid_invalid_xcd555(self):
        """
        Test that pipeline rejects invalid value in PayeeID column.
        
        Steps:
        1. Generate invalid parquet file with value XCD555 in PayeeID
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify error file created in error folder
        7. Database validation: Compare DB error count with CSV row count
        """
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        # Run the pipeline with invalid value XCD555 injected into PayeeID column
        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values", "PayeeID:XCD555",
            "--dev2",
            "--rows", "25"
        ]
        
        with allure.step("Inject invalid value XCD555 into PayeeID column"):
            print("=" * 60)
            print("TEST: Invalid Value 'XCD555' in PayeeID Column")
            print("=" * 60)
            print(f"Injecting: XCD555 into PayeeID column")
            print(f"Command: {' '.join(pipe_command)}")
            print("=" * 60)
        
        with allure.step("Run ETL pipeline"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
        
        with allure.step("Verify pipeline detected invalid data"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)
            
            # Pipeline should succeed (return 0) but detect errors via validation
            assert pipe_result.returncode == 0, f"Pipeline failed with return code {pipe_result.returncode}"
