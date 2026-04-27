import sys
import os
import subprocess
import allure

"""
Combined CHK scenario covering string-compatible generated schema columns in one file.

Rows 0-29 each target one column with a row-specific invalid value so
debug output can be mapped directly to a single field mutation.
"""


@allure.feature("ETL Validation")
@allure.story("Invalid Data Handling")
@allure.title("CHK Combined - Core Special Characters + Contact Max Length")
class TestChkCombinedSpecialCharsAndMaxLength:

    @allure.description(
        """
    Combined CHK validation covering string-compatible generated schema columns in a single parquet upload.

    Coverage model:
    - Force CHK baseline context and D/P-style org context for consistent behavior.
    - Inject one invalid value per row across rows 0-29 to isolate field failures.
    - Preserve row-level traceability through debug artifacts.

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
          # Baseline deterministic context
          "OrganizationCode:D",
          "OrganizationCode:0=P",
          "OrganizationCode:1=P",
            "PaymentMode:CHK",
          # String-column coverage: one primary invalid mutation per row
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
          "PaymentMode:10=XYZ",
          "RoutingTransitNumber:11=12345ABCD",
          "AccountNumber:12=12#456",
          "AccountType:13=CHECK",
          "AddressCode:16=***",
          "AddressLine1:17=AddressLine1ValueWith#Special",
          "AddressLine2:18=AddressLine2ValueWith@Special",
          "CityName:19=City!",
          "State:20=C1",
          "PostalCode:21=12#45",
          "ContactCode:22=CONTACT_CODE_TOO_LONG",
          "ContactFirstName:23=ContactFirstNameExceedingMaximumAllowedLengthValue",
          "ContactLastName:24=ContactLastNameExceedingMaximumAllowedLengthValue",
          "ContactTitle:25=ContactTitleExceedingMaximumAllowedLength",
          "ContactPhone:26=123ABC4567",
          "ContactFax:27=123ABC4567",
          "ContactOtherPhone:28=123ABC4567",
          "ContactEmail:29=verylongemailaddressthatexceedsmaximumlengthvalidationthresholdof100charactersexactly12345@example.com",
            "--dev2",
          "--rows", "34",
            "--test-name", "test_chk_combined_special_chars_and_max_length",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
