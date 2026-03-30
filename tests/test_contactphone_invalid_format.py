import sys
import os
import subprocess
import re
import allure

"""
Test Run: ContactPhone Invalid Format - Special Characters Not Allowed
ContactPhone field validation for D/P organizations with format constraints.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactPhone Invalid Format - Special Characters')
class TestContactPhoneInvalidFormat:

    @allure.description("""
    Test ContactPhone field format validation for invalid characters.

    Notes:
    - ContactPhone is a required contact field for D/P organizations
    - Maximum length is 25 characters
    - Must reject special characters and invalid formats
    - ContactPhone is optional for M organizations
    - Test validates rejection of values with special chars (#, $, %, etc)

    Steps:
    1. Generate parquet file with ContactPhone containing special characters
    2. Set ContactPhone to value with invalid chars (e.g., "555-123-4567#")
    3. Force D/P org context (contact fields required for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of ContactPhone values with invalid format/special chars
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contactphone_invalid_format(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactPhone:555-123-4567#",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_contactphone_invalid_format"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
