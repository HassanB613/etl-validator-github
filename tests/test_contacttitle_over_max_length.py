import sys
import os
import subprocess
import re
import allure

"""
Test Run: ContactTitle Over Max Length - Must Be <= 25 Characters
ContactTitle field required for D/P organizations with length validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactTitle Over Max Length')
class TestContactTitleOverMaxLength:

    @allure.description("""
    Test ContactTitle field length validation exceeding maximum.

    Notes:
    - ContactTitle is a required contact field for D/P organizations
    - Maximum length is 25 characters
    - ContactTitle is optional for M organizations
    - Test validates rejection of values exceeding 25-char limit

    Steps:
    1. Generate parquet file with ContactTitle exceeding max length
    2. Set ContactTitle to 26+ character string
    3. Force D/P org context (contact fields required for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of ContactTitle values > 25 characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contacttitle_over_max_length(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactTitle:VeryLongContactTitleExceedingMaxLength",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_contacttitle_over_max_length"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
