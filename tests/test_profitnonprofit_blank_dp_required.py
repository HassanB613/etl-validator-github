import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank ProfitNonprofit with D/P Org Type (Required Context)
Forces OrganizationCode=D or P where ProfitNonprofit is required.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test ProfitNonprofit Blank with D/P Org Type')
class TestProfitNonprofitBlankDPRequired:

    @allure.description("""
    Test blank ProfitNonprofit field behavior when required for D/P org types.

    Notes:
    - For OrganizationCode=D and P, ProfitNonprofit is REQUIRED.
    - For OrganizationCode=M, ProfitNonprofit is optional.
    - Blank values should trigger validation errors for D/P.

    Steps:
    1. Generate parquet file with blank ProfitNonprofit values
    2. Force D/P org context (OrgCode=D or P only)
    3. Set ProfitNonprofit to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling for required ProfitNonprofit in D/P context
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_profitnonprofit_blank_dp_required(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ProfitNonprofit:",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_profitnonprofit_blank_dp_required"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

