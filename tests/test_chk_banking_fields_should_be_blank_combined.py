import sys
import os
import subprocess
import allure

"""
Combined CHK scenario for conditional banking blank-field rules.

Rows 0-2 each target one CHK-only blank rule so failures are isolated by row.
"""


@allure.feature("ETL Validation")
@allure.story("Conditional Business Rules")
@allure.title("CHK Combined - Banking Fields Should Be Blank")
class TestChkBankingFieldsShouldBeBlankCombined:

    @allure.description(
        """
    Validates CHK conditional rules for banking fields in one combined run.

    Rules:
    - For PaymentMode=CHK, RoutingTransitNumber should be blank.
    - For PaymentMode=CHK, AccountNumber should be blank.
    - For PaymentMode=CHK, AccountType should be blank.

    Implementation:
    - Force D/P organization context for deterministic behavior.
    - Force PaymentMode=CHK for all rows.
    - Inject one invalid banking field per dedicated row.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_chk_banking_fields_should_be_blank_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:CHK",
            "RoutingTransitNumber:0=123456789",
            "AccountNumber:1=123456789",
            "AccountType:2=CHKING",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_chk_banking_fields_should_be_blank_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
