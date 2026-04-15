import sys
import os
import subprocess
import allure

"""
Combined CHK scenario:
RecordOperation, OrganizationCode, PayeeID, OrganizationIdentifier,
OrganizationName, OrganizationLegalName, OrganizationTIN, OrganizationTINType,
ProfitNonprofit, and OrganizationNPI should reject special characters.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('CHK Combined - Core Org Fields Reject Special Characters')
class TestChkCoreFieldsSpecialCharactersCombined:

    @allure.description("""
    Validates that core organization fields reject special characters in CHK context.

    Target fields:
    - RecordOperation
    - OrganizationCode
    - PayeeID
    - OrganizationIdentifier
    - OrganizationName
    - OrganizationLegalName
    - OrganizationTIN
    - OrganizationTINType
    - ProfitNonprofit
    - OrganizationNPI

    Implementation:
    - Force PaymentMode=CHK for all rows.
    - Use row-specific invalid value injection so each targeted field is isolated
      to a dedicated row for easier troubleshooting.
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_chk_core_fields_special_characters_combined(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable,
            pipeline_path,
            "--invalid-values",
            "PaymentMode:CHK",
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
            "--dev2",
            "--rows", "12",
            "--test-name", "test_chk_core_fields_special_characters_combined",
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
