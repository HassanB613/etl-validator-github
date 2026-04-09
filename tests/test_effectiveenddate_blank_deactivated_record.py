import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank EffectiveEndDate with Deactivated Record Operation (Required Context)
All records with RecordOperation=D must have an Effective End Date.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test EffectiveEndDate Blank with Deactivated Record')
class TestEffectiveEndDateBlankDeactivatedRecord:

    @allure.description("""
    Test blank EffectiveEndDate field behavior when record is deactivated (RecordOperation=D).

    Notes:
    - All Deactivated records (RecordOperation=D) must have an EffectiveEndDate.
    - If blank, system should reject the record or default to current date.
    - Test validates that blank end dates are caught for deactivation operations.

    Steps:
    1. Generate parquet file with blank EffectiveEndDate values
    2. Force RecordOperation=D for all rows
    3. Set EffectiveEndDate to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling or default behavior for deactivated records
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_effectiveenddate_blank_deactivated_record(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "RecordOperation:D",
            "EffectiveEndDate:",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_effectiveenddate_blank_deactivated_record"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

