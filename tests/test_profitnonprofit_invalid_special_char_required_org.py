import sys
import os
import subprocess
import allure

"""
Test Run: Invalid ProfitNonprofit Special Characters (Deterministic Required Org Context)
Forces OrganizationCode to D/P so ProfitNonprofit is required for every row.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ProfitNonprofit Invalid Special Characters with Required Org Context')
class TestProfitNonprofitSpecialCharRequiredOrg:

    @allure.description("""
    Test invalid ProfitNonprofit behavior in a forced, deterministic required context.

    Notes:
    - All rows are constrained to required org types (D baseline with row overrides to P).
    - This removes mixed-org ambiguity where M can treat ProfitNonprofit as optional.
    - Full-row rejection is expected for invalid ProfitNonprofit value.
    - Expected result target is full impact (for example, DB=25, CSV=25 for 25 rows).

    Steps:
    1. Generate invalid parquet file
    2. Force required org context (D/P only)
    3. Inject invalid special characters in ProfitNonprofit
    4. Upload/trigger ETL pipeline
    5. Verify deterministic invalid handling in validation output
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_profitnonprofit_invalid_special_char_required_org(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ProfitNonprofit:@!#$&&",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_profitnonprofit_invalid_special_char_required_org"
        ]

        with allure.step("Run ETL pipeline with forced required org context"):
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
