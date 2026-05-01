import os
import subprocess
import sys

import allure

"""
Combined D/P EFT scenario for AccountType value-rule validation.

Rows 0-2 each target one AccountType value violation for row-level isolation.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("EFT Combined - AccountType Value Rules")
class TestAccountTypeValueRulesEftCombined:

    @allure.description(
        """
    Validates AccountType value rules in a deterministic EFT context.

    Rules:
    - AccountType must be one of allowed enum values in EFT banking context.
    - Lowercase and special-character values should be rejected.

    Implementation:
    - Force D/P organization context and PaymentMode=EFT.
    - Set valid routing/account companion fields for deterministic behavior.
    - Inject one invalid AccountType value per dedicated row.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accounttype_value_rules_eft_combined(self):
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
            "AccountType:0=checking",
            "AccountType:1=savings",
            "AccountType:2=$%$%$%$%",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_accounttype_value_rules_eft_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"