import sys
import os
import subprocess
import re
import allure

"""
Test Run: ContactLastName Invalid - Numeric Characters Not Allowed
ContactLastName field required for D/P organizations with format validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactLastName Invalid - Numeric Characters')
class TestContactLastNameInvalidNumeric:

    @allure.description("""
    Test ContactLastName field format validation rejecting numeric values.

    Notes:
    - ContactLastName is a required contact field for D/P organizations
    - Maximum length is 50 characters
    - Must reject numeric characters and special symbols
    - ContactLastName is optional for M organizations
    - Test validates rejection of names containing digits (e.g., "Smith456")

    Steps:
    1. Generate parquet file with ContactLastName containing numeric chars
    2. Set ContactLastName to value with digits (e.g., "Smith456")
    3. Force D/P org context (contact fields required for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of ContactLastName values with numeric characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contactlastname_invalid_numeric(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactLastName:Smith456",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_contactlastname_invalid_numeric"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

