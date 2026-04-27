import sys
import os
import subprocess
import allure

"""
TC58: AccountNumber should be blank for PaymentMode=CHK.
"""


@allure.feature('ETL Validation')
@allure.story('Conditional Business Rules')
@allure.title('TC58 - CHK Account Number Should Be Blank')
class TestAccountNumberChkShouldBeBlank:

    @allure.description("""
    Validates CHK conditional rule for account number.

    Rule:
    - For PaymentMode=CHK, AccountNumber should be blank.

    Scenario:
    - Force D/P organization context
    - Force PaymentMode=CHK
    - Set AccountNumber to non-blank value
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accountnumber_chk_should_be_blank(self):
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
            "AccountNumber:123456789",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_accountnumber_chk_should_be_blank",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
        assert False, "Intentional failure requested for TestRail validation"
