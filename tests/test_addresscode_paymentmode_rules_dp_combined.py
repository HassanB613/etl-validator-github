import sys
import os
import subprocess
import allure

"""
Combined D/P AddressCode payment-mode rule validation.

Rows 0-1 isolate CHK and EFT AddressCode rule violations in one run.
"""


@allure.feature('ETL Validation')
@allure.story('Conditional Business Rules')
@allure.title('Combined - AddressCode Rules by PaymentMode (D/P)')
class TestAddressCodePaymentModeRulesDpCombined:

    @allure.description("""
    Validates AddressCode conditional rules for D/P records in one combined test.

    Rules:
    - For PaymentMode=CHK with OrganizationCode D/P, AddressCode must be PMT.
    - For PaymentMode=EFT with OrganizationCode D/P, AddressCode must be COR.

    Implementation:
    - Force D/P context across generated rows.
    - Use row-specific overrides to run both invalid scenarios in one upload.
      - Row 0: CHK + AddressCode=COR (invalid)
      - Row 1: EFT + AddressCode=PMT (invalid)
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_addresscode_paymentmode_rules_dp_combined(self):
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
            "AddressCode:PMT",
            "AddressCode:0=COR",
            "PaymentMode:1=EFT",
            "AddressCode:1=PMT",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_addresscode_paymentmode_rules_dp_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
