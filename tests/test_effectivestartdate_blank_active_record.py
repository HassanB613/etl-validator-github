import sys
import os
import subprocess
import re
import allure

"""
Test Run: Blank EffectiveStartDate with Active Record Operation (Required Context)
All records with RecordOperation=A must have a Effective Start Date.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test EffectiveStartDate Blank with Active Record')
class TestEffectiveStartDateBlankActiveRecord:

    @allure.description("""
    Test blank EffectiveStartDate field behavior when record is active (RecordOperation=A).

    Notes:
    - All Active records (RecordOperation=A) must have an EffectiveStartDate.
    - If blank, system should default it to current date.
    - Test validates that blank start dates are caught or properly defaulted.

    Steps:
    1. Generate parquet file with blank EffectiveStartDate values
    2. Force RecordOperation=A for all rows
    3. Set EffectiveStartDate to blank
    4. Upload/trigger ETL pipeline
    5. Verify error handling or default behavior for active records
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_effectivestartdate_blank_active_record(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "RecordOperation:A",
            "EffectiveStartDate:",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_effectivestartdate_blank_active_record"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"

