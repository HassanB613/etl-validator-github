import sys
import os
import subprocess
import re
import allure

"""
Test Run: Invalid OrganizationTINType (Invalid Value)
Valid values are EIN or SSN only. Tests rejection of invalid codes.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test OrganizationTINType Invalid Value')
class TestOrganizationTINTypeInvalidValue:

    @allure.description("""
    Test invalid OrganizationTINType field behavior.

    Notes:
    - Valid values are: EIN (Entity) or SSN (Individual).
    - All other values should trigger validation errors.
    - Test injects invalid value not matching allowed list.

    Steps:
    1. Generate parquet file with invalid OrganizationTINType values
    2. Set OrganizationTINType to invalid code (e.g., XXX)
    3. Upload/trigger ETL pipeline
    4. Verify error handling for TINType not in [EIN, SSN]
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_organizationtintype_invalid_value(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationTINType:XXX",
            "--dev2",
            "--rows", "25",
            "--test-name", "test_organizationtintype_invalid_value"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
