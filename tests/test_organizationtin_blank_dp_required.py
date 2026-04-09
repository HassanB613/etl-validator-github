import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank OrganizationTIN with D/P Org Type (Required Context)
Forces OrganizationCode=D or P where TIN is required (9 digits).
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test OrganizationTIN Blank with D/P Org Type')
class TestOrganizationTINBlankDPRequired:

    @allure.description("""
    Test blank OrganizationTIN field behavior when required for D/P org types.

    Notes:
    - For OrganizationCode=D and P, TIN is REQUIRED (9 digits).
    - For OrganizationCode=M, TIN is optional (all 9s defaults to empty space).
    - Blank values should trigger validation errors for D/P.

    Steps:
    1. Generate parquet file with blank OrganizationTIN values
    2. Force D/P org context (OrgCode=D or P only)
    3. Set OrganizationTIN to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling for required TIN in D/P context
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_organizationtin_blank_dp_required(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "OrganizationTIN:",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_organizationtin_blank_dp_required"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

