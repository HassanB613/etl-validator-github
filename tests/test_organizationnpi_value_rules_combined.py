import os
import subprocess
import sys

import allure

"""
Combined OrganizationNPI value-rule validation.

Rows 0-1 isolate invalid OrganizationNPI patterns in one run.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("Combined - OrganizationNPI Value Rules")
class TestOrganizationNpiValueRulesCombined:

    @allure.description(
        """
    Validates OrganizationNPI value rules in one combined test.

    Rules:
    - OrganizationNPI must be numeric.
    - OrganizationNPI must meet expected length constraints.

    Implementation:
    - Use row-specific invalid values to isolate each rule violation.
    - Keep all mutations in one upload for faster regression coverage.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_organizationnpi_value_rules_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "OrganizationNPI:0=1",
            "OrganizationNPI:1=XxXyYyZzZ",
            "--dev2",
            "--rows", "10",
            "--test-name", "test_organizationnpi_value_rules_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"