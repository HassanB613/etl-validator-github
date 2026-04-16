import sys
import os
import subprocess
import allure

"""
Combined CHK scenario — two test groups in one file:

Group 1 (rows 0-9): Core org fields reject special characters
  RecordOperation, OrganizationCode, PayeeID, OrganizationIdentifier,
  OrganizationName, OrganizationLegalName, OrganizationTIN, OrganizationTINType,
  ProfitNonprofit, OrganizationNPI

Group 2 (rows 10-17): Contact fields over max length
  ContactCode, ContactFirstName, ContactLastName, ContactTitle,
  ContactPhone, ContactFax, ContactOtherPhone, ContactEmail
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("CHK Combined - Core Special Characters + Contact Max Length")
class TestChkCombinedSpecialCharsAndMaxLength:

    @allure.description(
        """
    Combined CHK validation covering two scenario groups in a single parquet upload.

    Group 1 — Core org fields with special characters (rows 0-9):
    - Row 0:  RecordOperation      = $
    - Row 1:  OrganizationCode     = @
    - Row 2:  PayeeID              = #$%
    - Row 3:  OrganizationIdentifier = *&^
    - Row 4:  OrganizationName     = Name!
    - Row 5:  OrganizationLegalName = Legal@
    - Row 6:  OrganizationTIN      = 12#456789
    - Row 7:  OrganizationTINType  = @#$
    - Row 8:  ProfitNonprofit      = %
    - Row 9:  OrganizationNPI      = 12@3456789

    Group 2 — Contact fields over max length (rows 10-17):
    - Row 10: ContactCode          = CONTACT_CODE_TOO_LONG
    - Row 11: ContactFirstName     = ContactFirstNameExceedingMaximumAllowedLengthValue
    - Row 12: ContactLastName      = ContactLastNameExceedingMaximumAllowedLengthValue
    - Row 13: ContactTitle         = ContactTitleExceedingMaximumAllowedLength
    - Row 14: ContactPhone         = 12345678901234567890
    - Row 15: ContactFax           = 12345678901234567890
    - Row 16: ContactOtherPhone    = 12345678901234567890
    - Row 17: ContactEmail         = verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com

    Implementation:
    - Force PaymentMode=CHK for all rows.
    - Each injected field is isolated to a dedicated row for easy per-row debugging.
    - Debug parquet (with test_case_name column) saved to evidence folder.
    - Column stripped before S3 upload.
    """
    )
    @allure.severity(allure.severity_level.CRITICAL)
    def test_chk_combined_special_chars_and_max_length(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "PaymentMode:CHK",
            # Group 1: special characters — rows 0-9
            "RecordOperation:0=$",
            "OrganizationCode:1=@",
            "PayeeID:2=#$%",
            "OrganizationIdentifier:3=*&^",
            "OrganizationName:4=Name!",
            "OrganizationLegalName:5=Legal@",
            "OrganizationTIN:6=12#456789",
            "OrganizationTINType:7=@#$",
            "ProfitNonprofit:8=%",
            "OrganizationNPI:9=12@3456789",
            # Group 2: contact max length — rows 10-17
            "ContactCode:10=CONTACT_CODE_TOO_LONG",
            "ContactFirstName:11=ContactFirstNameExceedingMaximumAllowedLengthValue",
            "ContactLastName:12=ContactLastNameExceedingMaximumAllowedLengthValue",
            "ContactTitle:13=ContactTitleExceedingMaximumAllowedLength",
            "ContactPhone:14=12345678901234567890",
            "ContactFax:15=12345678901234567890",
            "ContactOtherPhone:16=12345678901234567890",
            "ContactEmail:17=verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com",
            "--dev2",
            "--rows", "20",
            "--test-name", "test_chk_combined_special_chars_and_max_length",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
