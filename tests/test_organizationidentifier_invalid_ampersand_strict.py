import sys
import os
import subprocess
import allure

"""
Test Run: Invalid OrganizationIdentifier (Ampersand) in Deterministic Required Context
Forces OrganizationCode=D relationship path so OrganizationIdentifier rules are consistently enforced.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test OrganizationIdentifier Invalid Ampersand (Strict Required Context)')
class TestOrganizationIdentifierAmpersandStrict:

    @allure.description("""
    Test invalid OrganizationIdentifier behavior in a forced, deterministic required context.

    Notes:
    - All rows are constrained to OrganizationCode=D (D/P/M same-rule path).
    - PayeeID is set to a valid constant while OrganizationIdentifier is set to invalid ampersand content.
    - This removes mixed org-type ambiguity and is intended to produce consistent full-row rejection.
    - Expected result target is full impact (for example, DB=25, CSV=25 for 25 rows).

    Steps:
    1. Generate invalid parquet file
    2. Force deterministic org context and relationship path
    3. Inject invalid OrganizationIdentifier value
    4. Upload/trigger ETL pipeline
    5. Verify deterministic invalid handling in validation output
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_organizationidentifier_invalid_ampersand_strict(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "PayeeID:PAYEE123",
            "OrganizationIdentifier:&&&&&&&",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_organizationidentifier_invalid_ampersand_strict"
        ]

        with allure.step("Run ETL pipeline in deterministic OrganizationIdentifier-required context"):
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
