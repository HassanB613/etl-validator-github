import sys
import os
import subprocess
import allure

"""
Test Run: Too Short Value in AccountNumber Column (Deterministic EFT/Required Context)
Forces PaymentMode=EFT and required org context (M/P) so AccountNumber is mandatory for every row.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test AccountNumber Too Short (8) with EFT + Required Org Context')
class TestAccountNumberInvalidSingleDigitEFTRequired:

    @allure.description("""
    Test invalid AccountNumber behavior in a forced, deterministic required context.

    Notes:
    - All rows are constrained to PaymentMode=EFT and required org types (M/P).
    - This removes mixed-rule ambiguity (such as CHK behavior and R optional handling).
    - Full-row rejection is expected for bad AccountNumber input.

    Steps:
    1. Generate invalid parquet file
    2. Force required context (OrgCode=P baseline, row overrides to M, PaymentMode=EFT)
    3. Set required companion fields and inject AccountNumber=8
    4. Upload/trigger ETL pipeline
    5. Verify deterministic invalid handling in validation output
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_accountnumber_invalid_single_digit_eft_required(self):
        """
        Deterministic companion test for AccountNumber invalid single-digit validation.
        """

        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:P",
            "OrganizationCode:0=M",
            "OrganizationCode:1=M",
            "PaymentMode:EFT",
            "AccountType:CHKING",
            "RoutingTransitNumber:123456789",
            "AccountNumber:8",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_accountnumber_invalid_single_digit_eft_required"
        ]

        with allure.step("Run ETL pipeline with forced EFT/required org context"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)

        with allure.step("Verify deterministic validation output"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)

            assert pipe_result.returncode == 0, f"Pipeline crashed with return code {pipe_result.returncode}"
            assert "Row counts MATCH" in pipe_result.stdout, (
                f"Validation failed - output should contain 'Row counts MATCH', but got: {pipe_result.stdout[-500:]}"
            )
            assert "DB=25, CSV=25" in pipe_result.stdout, (
                "Expected deterministic full-row impact (DB=25, CSV=25) was not found in output."
            )
