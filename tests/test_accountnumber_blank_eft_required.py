import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank AccountNumber with EFT Payment Mode (Deterministic Context)
Forces PaymentMode=EFT and D/P org context so AccountNumber is required for every row.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test AccountNumber Blank with EFT + D/P Context')
class TestAccountNumberBlankEFTRequired:

    @allure.description("""
    Test blank AccountNumber field behavior when banking fields are required (EFT payment mode, D/P org).

    Notes:
    - All rows are constrained to PaymentMode=EFT and D/P org context.
    - AccountNumber field is required in this context (1-17 characters).
    - Blank values should trigger validation errors.

    Steps:
    1. Generate parquet file with blank AccountNumber values
    2. Force required context (OrgCode=D/P, PaymentMode=EFT)
    3. Set AccountNumber to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling for blank AccountNumber in required context
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accountnumber_blank_eft_required(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "AccountNumber:",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_accountnumber_blank_eft_required"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

