import sys
import os
import subprocess
import allure

"""
Test Run: Banking Extras Format Validation - Combined
RoutingTransitNumber leading zeros padding and AccountNumber minimal length validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test Banking Extras Format Validation Combined')
class TestBankingExtrasFormatValidationCombined:

    @allure.description("""
    Test banking field format edge cases: RoutingTransitNumber padding and AccountNumber minimal length.

    Notes:
    - RoutingTransitNumber must be exactly 9 numeric digits; values < 9 digits require leading zeros
    - AccountNumber must meet minimum length requirements; single-digit values are invalid
    - Test uses row-isolated invalid values to validate format enforcement in one run
    - D/P org context with EFT PaymentMode ensures banking field validation is active
    
    Test Cases:
    - Row 0: RoutingTransitNumber with leading zeros (short value "54321", should require padding to "000054321")
    - Row 1: AccountNumber with single digit "8" (too short, should fail validation)

    Steps:
    1. Generate parquet file with 10 rows
    2. Set rows 0-1 with invalid banking field formats
    3. Force EFT payment mode and D/P org context for banking validation
    4. Upload/trigger ETL pipeline
    5. Verify pipeline detected format violations in both rows
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_banking_extras_format_validation(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PaymentMode:EFT",
            "RoutingTransitNumber:0=54321",        # Row 0: short routing (needs leading zeros)
            "AccountNumber:1=8",                    # Row 1: single-digit account (too short)
            "--dev2",
            "--rows", "10",
            "--test-name", "test_banking_extras_format_validation"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
