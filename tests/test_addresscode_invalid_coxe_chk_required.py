import sys
import os
import subprocess
import allure

"""
Test Run: Invalid Value in AddressCode Column (Deterministic CHK/Required Context)
Forces PaymentMode=CHK and D/P org context so address fields are required for every row.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test AddressCode Invalid Value (COXE) with CHK + D/P Context')
class TestAddressCodeInvalidCOXECHKRequired:

    @allure.description("""
    Test invalid AddressCode behavior in a forced, deterministic required context.

    Notes:
    - All rows are constrained to PaymentMode=CHK and supported org context (D/P).
    - This removes mixed-rule ambiguity from M/R rows where address fields may be blank.
    - Full-row rejection is expected when invalid AddressCode is injected.

    Steps:
    1. Generate invalid parquet file
    2. Force required context (OrgCode=D baseline, row overrides to P, PaymentMode=CHK)
    3. Inject AddressCode=COXE
    4. Upload/trigger ETL pipeline
    5. Verify deterministic invalid handling in validation output
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_addresscode_invalid_coxe_chk_required(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:CHK",
            "AddressCode:COXE",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_addresscode_invalid_coxe_chk_required"
        ]

        with allure.step("Run ETL pipeline with forced CHK + D/P context"):
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
