import os
import subprocess
import sys

import allure

"""
Combined D/P EFT scenario for banking-field format rules.

Rows 0-5 each target one banking-field format violation so failures are isolated by row.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("EFT Combined - Banking Field Format Rules")
class TestEftBankingFormatRulesCombined:

    @allure.description(
        """
    Validates EFT banking-field format rules in one combined run.

    Rules:
    - RoutingTransitNumber must be 9 numeric digits.
    - AccountNumber must be within valid length and character constraints.

    Implementation:
    - Force D/P organization context for deterministic behavior.
    - Force PaymentMode=EFT for all rows.
    - Provide valid companion banking values by default.
    - Inject one invalid banking-field format mutation per dedicated row.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_eft_banking_format_rules_combined(self):
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
            "RoutingTransitNumber:123456789",
            "AccountNumber:123456789",
            "AccountType:CHKING",
            "RoutingTransitNumber:0=12345A678",
            "RoutingTransitNumber:1=101",
            "RoutingTransitNumber:2=@#$&^%!!",
            "AccountNumber:3=8",
            "AccountNumber:4=!!@@$$&&",
            "AccountNumber:5=123456789012345678",
            "--dev2",
            "--rows", "12",
            "--test-name", "test_eft_banking_format_rules_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"