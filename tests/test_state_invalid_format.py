import sys
import os
import subprocess
import re
import allure

"""
Test Run: State Invalid Format - Reject Non-2-Character Values
State field must be exactly 2 characters (no validation of whether it IS a valid US state code).
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test State Invalid Format - Non 2-Char Rejection')
class TestStateInvalidFormat:

    @allure.description("""
    Test State field validation for non-2-character values.

    Notes:
    - State field must be exactly 2 characters long
    - Does NOT validate against US state code list (e.g., CA, TX, NY are valid format)
    - Rejects single-char values, 3+ char values, numeric values, special characters
    - Test uses invalid value "STATE" (5 chars - too long)

    Steps:
    1. Generate parquet file with invalid State values
    2. Set State to non-2-char value (e.g., "STATE" or "S" or "123")
    3. Force org context (D/P) for address field validation
    4. Upload/trigger ETL pipeline
    5. Verify rejection of non-2-char State values
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_state_invalid_format(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "State:STATE",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_state_invalid_format"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
