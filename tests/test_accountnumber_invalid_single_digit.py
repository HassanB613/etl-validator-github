import sys
import os
import subprocess
import pytest
import allure

"""
Test Run: Too Short Value in AccountNumber Column
Injects too short value '8' into the AccountNumber column to test ETL validation.
"""

@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test AccountNumber Column with Too Short Value (8)')
class TestAccountNumberInvalidSingleDigit:
    
    @allure.description("""
    Test invalid AccountNumber behavior under mixed, production-like data conditions.

    Notes:
    - PaymentMode=EFT is the key trigger where account fields are mandatory.
    - PaymentMode=CHK typically does not fail for bad/missing account numbers because those fields are expected blank.
    - Org-type rules add nuance: R rows may allow blank/optional account fields, while M and P generally require them.
    - Because of this conditional enforcement, partial outcomes such as DB=12, CSV=12 out of 25 are expected and valid.
    
    Steps:
    1. Generate invalid parquet file with value '8' in AccountNumber
    2. Upload to S3 ready folder
    3. Trigger/monitor Glue job
    4. Verify file removed from ready folder
    5. Verify file moved to archive folder
    6. Verify error file created in error folder
    7. Database validation: Compare DB error count with CSV row count
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accountnumber_invalid_single_digit(self):
        """
        Test invalid AccountNumber behavior under mixed, production-like data conditions.

        Notes:
        - PaymentMode=EFT is the key trigger where account fields are mandatory.
        - PaymentMode=CHK typically does not fail for bad/missing account numbers because those fields are expected blank.
        - Org-type rules add nuance: R rows may allow blank/optional account fields, while M and P generally require them.
        - Because of this conditional enforcement, partial outcomes such as DB=12, CSV=12 out of 25 are expected and valid.
        
        Steps:
        1. Generate invalid parquet file with value '8' in AccountNumber
        2. Upload to S3 ready folder
        3. Trigger/monitor Glue job
        4. Verify file removed from ready folder
        5. Verify file moved to archive folder
        6. Verify error file created in error folder
        7. Database validation: Compare DB error count with CSV row count
        """
        
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        # Run the pipeline with too short value '8' injected into AccountNumber column
        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values", "AccountNumber:8",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_accountnumber_invalid_single_digit"
        ]
        
        with allure.step("Inject too short value '8' into AccountNumber column"):
            print("=" * 60)
            print("TEST: Too Short Value '8' in AccountNumber Column")
            print("=" * 60)
            print(f"Injecting: 8 into AccountNumber column")
            print(f"Command: {' '.join(pipe_command)}")
            print("=" * 60)
        
        with allure.step("Run ETL pipeline"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)
        
        with allure.step("Verify pipeline detected invalid data"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)
            
            # Pipeline should complete without error
            assert pipe_result.returncode == 0, f"Pipeline crashed with return code {pipe_result.returncode}"
            
            # Verify validation detected errors (check for success indication)
            assert "Row counts MATCH" in pipe_result.stdout, f"Validation failed - output should contain 'Row counts MATCH', but got: {pipe_result.stdout[-500:]}"
