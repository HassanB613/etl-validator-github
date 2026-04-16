import os
import subprocess
import sys

import allure

"""
Combined CHK scenario:
ContactCode, ContactFirstName, ContactLastName, ContactTitle,
ContactPhone, ContactFax, ContactOtherPhone, and ContactEmail
should reject values beyond max-length limits.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("CHK Combined - Contact Fields Over Max Length")
class TestChkContactFieldsOverMaxLengthCombined:

    @allure.description(
        """
    Validates CHK contact-field max-length handling for a combined scenario.

    Target fields:
    - ContactCode
    - ContactFirstName
    - ContactLastName
    - ContactTitle
    - ContactPhone
    - ContactFax
    - ContactOtherPhone
    - ContactEmail

    Implementation:
    - Force PaymentMode=CHK for all rows.
    - Use row-specific over-max values so each field is isolated
      to a dedicated row for easier troubleshooting.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_chk_contact_fields_over_max_length_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "PaymentMode:CHK",
            "ContactCode:0=CONTACT_CODE_TOO_LONG",
            "ContactFirstName:1=ContactFirstNameExceedingMaximumAllowedLengthValue",
            "ContactLastName:2=ContactLastNameExceedingMaximumAllowedLengthValue",
            "ContactTitle:3=ContactTitleExceedingMaximumAllowedLength",
            "ContactPhone:4=12345678901234567890",
            "ContactFax:5=12345678901234567890",
            "ContactOtherPhone:6=12345678901234567890",
            "ContactEmail:7=verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com",
            "--dev2",
            "--rows",
            "12",
            "--test-name",
            "test_chk_contact_fields_over_max_length_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
