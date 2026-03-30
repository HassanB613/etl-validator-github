import sys
import os
import subprocess
import re
import allure

"""
Test Run: RoutingNumber Leading Zeros - Append 0 If < 9 Digits
RoutingTransitNumber field validation for numeric format and length.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test RoutingNumber Leading Zeros - Pad to 9 Digits')
class TestRoutingNumberLeadingZeros:

    @allure.description("""
    Test RoutingTransitNumber padding logic for values with leading zeros.

    Notes:
    - RoutingTransitNumber must be exactly 9 numeric digits
    - If < 9 digits, system should prepend leading zeros (e.g., "12345" becomes "000012345")
    - Test validates that numeric values are properly padded to 9 digits
    - Test uses invalid value "54321" (5 digits - requires 4 leading zeros)

    Steps:
    1. Generate parquet file with short RoutingTransitNumber values
    2. Set RoutingTransitNumber to < 9 digits (e.g., "54321")
    3. Force EFT payment mode and D/P org context for banking validation
    4. Upload/trigger ETL pipeline
    5. Verify padding logic or rejection of improper routing numbers
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_routingnumber_leading_zeros(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "RoutingTransitNumber:54321",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_routingnumber_leading_zeros"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
