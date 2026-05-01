import os
import subprocess
import sys

import allure

"""
Combined D/P PaymentMode requiredness scenario.

Rows isolate CHK not-required behavior and EFT required-field failures.
"""


@allure.feature("ETL Validation")
@allure.story("Conditional Business Rules")
@allure.title("Combined - PaymentMode Requiredness Rules (D/P)")
class TestPaymentModeRequirednessRulesDpCombined:

    @allure.description(
        """
    Validates PaymentMode-driven requiredness rules for D/P records in one run.

    Rules:
    - For PaymentMode=CHK, banking fields can be blank.
    - For PaymentMode=EFT, routing and address fields are required.

    Implementation:
    - Force D/P context across generated rows.
    - Keep EFT-valid companion values as baseline.
    - Apply row-specific overrides to isolate CHK-valid and EFT-invalid cases.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_paymentmode_requiredness_rules_dp_combined(self):
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
            "AddressCode:COR",
            "PaymentMode:0=CHK",
            "RoutingTransitNumber:0=",
            "AccountNumber:0=",
            "AccountType:0=",
            "RoutingTransitNumber:1=",
            "AddressCode:2=",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_paymentmode_requiredness_rules_dp_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"