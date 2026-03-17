import sys
import os
import subprocess
import allure

"""
Test Run: ContactFirstName Blank (D/P Records)
Injects blank ContactFirstName for D/P records to validate min length/required boundary.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ContactFirstName Blank (D/P Records)')
class TestContactFirstNameBlankDP:

    @allure.description("""
    Test that pipeline rejects blank ContactFirstName for D/P records.

    Steps:
    1. Generate invalid parquet file
    2. Force OrganizationCode to D/P only
    3. Inject blank ContactFirstName
    4. Upload/trigger ETL pipeline
    5. Verify invalid handling in validation output
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contactfirstname_blank_dp(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactFirstName:",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_contactfirstname_blank_dp"
        ]

        with allure.step("Run ETL pipeline with blank ContactFirstName for D/P records"):
            pipe_result = subprocess.run(pipe_command, capture_output=True, text=True)

        with allure.step("Verify pipeline output"):
            allure.attach(pipe_result.stdout, "Pipeline Output", allure.attachment_type.TEXT)
            if pipe_result.stderr:
                allure.attach(pipe_result.stderr, "Pipeline Errors", allure.attachment_type.TEXT)

            assert pipe_result.returncode == 0, f"Pipeline crashed with return code {pipe_result.returncode}"
            assert "Row counts MATCH" in pipe_result.stdout, (
                f"Validation failed - output should contain 'Row counts MATCH', but got: {pipe_result.stdout[-500:]}"
            )
            assert "DB=0, CSV=0 (no error CSV generated)" not in pipe_result.stdout, (
                "Expected invalid D/P record handling, but pipeline reported no error records."
            )
