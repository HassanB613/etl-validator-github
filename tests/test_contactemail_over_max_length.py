import sys
import os
import subprocess
import re
import allure

"""
Test Run: ContactEmail Over Max Length - Must Be <= 100 Characters
ContactEmail field required for D/P organizations with max length validation only.
Per user clarification: No RFC format validation, only max length check.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactEmail Over Max Length')
class TestContactEmailOverMaxLength:

    @allure.description("""
    Test ContactEmail field length validation exceeding maximum.

    Notes:
    - ContactEmail is a required contact field for D/P organizations
    - Maximum length is 100 characters
    - ContactEmail is optional for M organizations
    - Validates only max length, NOT RFC 5322 email format compliance
    - Test validates rejection of values exceeding 100-char limit

    Steps:
    1. Generate parquet file with ContactEmail exceeding max length
    2. Set ContactEmail to 101+ character string
    3. Force D/P org context (contact fields required for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of ContactEmail values > 100 characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contactemail_over_max_length(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactEmail:verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_contactemail_over_max_length"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

