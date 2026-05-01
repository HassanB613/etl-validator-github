import os
import subprocess
import sys

import allure

"""
Combined D/P scenario:
ContactFirstName, ContactLastName, ContactTitle, and ContactEmail
should reject values beyond max-length limits.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("D/P Combined - Contact Fields Over Max Length")
class TestContactFieldsOverMaxLengthDPCombined:

    @allure.description(
        """
    Validates D/P contact-field max-length handling for a combined scenario.

    Target fields:
    - ContactFirstName
    - ContactLastName
    - ContactTitle
    - ContactEmail

    Implementation:
    - Force D/P organization context for all rows.
    - Use row-specific over-max values so each field is isolated
      to a dedicated row for easier troubleshooting.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contact_fields_over_max_length_dp_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactFirstName:0=ABCDEFGHIJKLMNOPQRSTU",
            "ContactLastName:1=ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "ContactTitle:2=VeryLongContactTitleExceedingMaxLength",
            "ContactEmail:3=verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com",
            "--dev2",
            "--rows",
            "10",
            "--test-name",
            "test_contact_fields_over_max_length_dp_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"