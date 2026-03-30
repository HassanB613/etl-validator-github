import sys
import os
import subprocess
import re
import allure

"""
Test Run: RoutingNumber Invalid Characters - Numeric Only
RoutingTransitNumber field required for EFT payment mode with format validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test RoutingNumber Invalid Characters')
class TestRoutingNumberInvalidCharacters:

    @allure.description("""
    Test RoutingTransitNumber field format validation rejecting non-numeric values.

    Notes:
    - RoutingTransitNumber must be exactly 9 numeric digits
    - Must reject alphabetic characters and special symbols
    - Required for EFT payment mode (PaymentMode=EFT)
    - Test validates rejection of routing numbers with invalid chars (e.g., "12345A678")

    Steps:
    1. Generate parquet file with RoutingTransitNumber containing non-numeric chars
    2. Set RoutingTransitNumber to value with letters/symbols (e.g., "12345A678")
    3. Force PaymentMode=EFT and D/P org context (banking validation)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of RoutingTransitNumber values with invalid characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_routingnumber_invalid_characters(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "RoutingTransitNumber:12345A678",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_routingnumber_invalid_characters"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
