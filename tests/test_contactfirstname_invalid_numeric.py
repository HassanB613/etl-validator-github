import sys
import os
import subprocess
import re
import allure

"""
Test Run: ContactFirstName Invalid - Numeric Characters Not Allowed
ContactFirstName field required for D/P organizations with format validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactFirstName Invalid - Numeric Characters')
class TestContactFirstNameInvalidNumeric:

    @allure.description("""
    Test ContactFirstName field format validation rejecting numeric values.

    Notes:
    - ContactFirstName is a required contact field for D/P organizations
    - Maximum length is 50 characters
    - Must reject numeric characters and special symbols
    - ContactFirstName is optional for M organizations
    - Test validates rejection of names containing digits (e.g., "John123")

    Steps:
    1. Generate parquet file with ContactFirstName containing numeric chars
    2. Set ContactFirstName to value with digits (e.g., "John123")
    3. Force D/P org context (contact fields required for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of ContactFirstName values with numeric characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contactfirstname_invalid_numeric(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactFirstName:John123",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_contactfirstname_invalid_numeric"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
