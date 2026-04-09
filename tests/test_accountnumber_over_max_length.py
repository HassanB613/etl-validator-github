import sys
import os
import subprocess
import re
import allure

"""
Test Run: AccountNumber Over Max Length - Must Be <= 17 Characters
AccountNumber field required for EFT payment mode with length validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test AccountNumber Over Max Length')
class TestAccountNumberOverMaxLength:

    @allure.description("""
    Test AccountNumber field length validation exceeding maximum.

    Notes:
    - AccountNumber is required for EFT payment mode (PaymentMode=EFT)
    - Valid length range: 1-17 characters
    - AccountNumber is not required for CHK payment mode
    - Test validates rejection of values exceeding 17-char limit

    Steps:
    1. Generate parquet file with AccountNumber exceeding max length
    2. Set AccountNumber to 18+ character string
    3. Force PaymentMode=EFT and D/P org context (banking validation)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of AccountNumber values > 17 characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accountnumber_over_max_length(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "AccountNumber:123456789012345678",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_accountnumber_over_max_length"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

