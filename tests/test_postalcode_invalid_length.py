import sys
import os
import subprocess
import re
import allure

"""
Test Run: PostalCode Invalid Length - Must Be 5-10 Characters
PostalCode field length validation for address records.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test PostalCode Invalid Length - Out of Range')
class TestPostalCodeInvalidLength:

    @allure.description("""
    Test PostalCode field length validation.

    Notes:
    - PostalCode must be between 5 and 10 characters
    - Must reject values < 5 chars (e.g., "12345" is min valid, "1234" is invalid)
    - Must reject values > 10 chars (e.g., "1234567890" is max valid, "12345678901" is invalid)
    - Test uses invalid value "123" (too short - 3 chars)

    Steps:
    1. Generate parquet file with invalid PostalCode values
    2. Set PostalCode length outside 5-10 range
    3. Force org context (D/P) for address field validation
    4. Upload/trigger ETL pipeline
    5. Verify rejection of out-of-range postal code lengths
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_postalcode_invalid_length(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PostalCode:123",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_postalcode_invalid_length"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
