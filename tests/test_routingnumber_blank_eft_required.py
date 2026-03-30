import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank RoutingTransitNumber with EFT Payment Mode (Deterministic Context)
Forces PaymentMode=EFT and D/P org context so RoutingTransitNumber is required for every row.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test RoutingTransitNumber Blank with EFT + D/P Context')
class TestRoutingNumberBlankEFTRequired:

    @allure.description("""
    Test blank RoutingTransitNumber field behavior when banking fields are required (EFT payment mode, D/P org).

    Notes:
    - All rows are constrained to PaymentMode=EFT and D/P org context.
    - RoutingTransitNumber field is required in this context (9 digits).
    - Blank values should trigger validation errors.

    Steps:
    1. Generate parquet file with blank RoutingTransitNumber values
    2. Force required context (OrgCode=D/P, PaymentMode=EFT)
    3. Set RoutingTransitNumber to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling for blank RoutingTransitNumber in required context
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_routingnumber_blank_eft_required(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "RoutingTransitNumber:",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_routingnumber_blank_eft_required"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
