import sys
import os
import subprocess
import re
import allure

"""
Test Run: State Invalid Single Digit - Must Be 2 Characters
State field validation rejecting values less than 2 characters.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test State Invalid Single Character')
class TestStateInvalidSingleCharacter:

    @allure.description("""
    Test State field validation rejecting single-character values.

    Notes:
    - State field must be exactly 2 characters long
    - Does NOT validate against US state code list
    - Must reject any value != 2 chars (single char, 3+ chars, etc)
    - Required for D/P address validation
    - Test validates rejection of single-char state values (e.g., "C" instead of "CA")

    Steps:
    1. Generate parquet file with State as single character
    2. Set State to 1-char value (e.g., "C" or "T")
    3. Force D/P org context (address fields validated for D/P)
    4. Upload/trigger ETL pipeline
    5. Verify rejection of State values with < 2 characters
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_state_invalid_single_character(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "State:C",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_state_invalid_single_character"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
