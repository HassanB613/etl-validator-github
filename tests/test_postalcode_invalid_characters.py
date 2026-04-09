import sys
import os
import subprocess
import re
import allure

"""
Test Run: PostalCode Invalid Characters - Alphanumeric Only
PostalCode field validation for address records with format constraints.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test PostalCode Invalid Characters - Special Chars')
class TestPostalCodeInvalidCharacters:

    @allure.description("""
    Test PostalCode field format validation rejecting special characters.

    Notes:
    - PostalCode must be between 5 and 10 characters
    - Alphanumeric format (no special characters)
    - Must reject special chars (#, @, &, %, etc)
    - Required for D/P address validation
    - Test validates rejection of postal codes with invalid chars (e.g., "90210#")

    Steps:
    1. Generate parquet file with PostalCode containing special characters
    2. Set PostalCode to value with invalid chars (e.g., "90210#")
    3. Force D/P org context (address fields validated for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of PostalCode values with special characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_postalcode_invalid_characters(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PostalCode:90210#",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_postalcode_invalid_characters"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

