import os
import subprocess
import sys

import allure

"""
Combined D/P EFT scenario for required banking fields.

Rows 0-2 each target one EFT-required blank-field rule so failures are isolated by row.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("EFT Combined - Banking Fields Blank But Required")
class TestEftBankingFieldsBlankRequiredCombined:

    @allure.description(
        """
    Validates EFT banking-field requiredness in one combined run.

    Rules:
    - For PaymentMode=EFT, RoutingTransitNumber is required.
    - For PaymentMode=EFT, AccountNumber is required.
    - For PaymentMode=EFT, AccountType is required.

    Implementation:
    - Force D/P organization context for deterministic behavior.
    - Force PaymentMode=EFT for all rows.
    - Inject one blank required banking field per dedicated row.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_eft_banking_fields_blank_required_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "RoutingTransitNumber:0=",
            "AccountNumber:1=",
            "AccountType:2=",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_eft_banking_fields_blank_required_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"