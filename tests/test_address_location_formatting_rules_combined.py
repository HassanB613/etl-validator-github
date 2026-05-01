import sys
import os
import subprocess
import allure

"""
Test Run: Address/Location Formatting Rules Combined
PostalCode length/format, State length format, and AddressCode enum rules validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test Address/Location Formatting Rules Combined')
class TestAddressLocationFormattingRulesCombined:

    @allure.description("""
    Test address and location field format rules: PostalCode alphanumeric + length, State 2-char format, AddressCode enum values.

    Notes:
    - PostalCode: 5-10 alphanumeric characters (no special chars, no invalid length)
    - State: Exactly 2 characters (format only, not state code validation)
    - AddressCode: Valid enum values (COXE is invalid; special chars rejected)
    - Test uses row-isolated invalid values to validate format enforcement in one run
    - D/P org context ensures address field validation is active
    
    Test Cases:
    - Row 0: PostalCode with special character "90210#" (alphanumeric required)
    - Row 1: PostalCode too short "123" (length must be 5-10)
    - Row 2: PostalCode with special chars "@#$%!!" (alphanumeric required)
    - Row 3: State value "STATE" too long (must be exactly 2 chars)
    - Row 4: State value "C" too short (must be exactly 2 chars)
    - Row 5: AddressCode invalid enum "COXE" (not a valid code)
    - Row 6: AddressCode with special chars "@#$%!!" (special chars rejected)

    Steps:
    1. Generate parquet file with 10 rows
    2. Set rows 0-6 with invalid address/location field formats
    3. Force D/P org context for address field validation
    4. Upload/trigger ETL pipeline
    5. Verify pipeline detected format violations in all 7 rows
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_address_location_formatting_rules(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "PostalCode:0=90210#",           # Row 0: special char in postal
            "PostalCode:1=123",              # Row 1: postal too short
            "PostalCode:2=@#$%!!",           # Row 2: special chars in postal
            "State:3=STATE",                 # Row 3: state too long
            "State:4=C",                     # Row 4: state too short
            "AddressCode:5=COXE",            # Row 5: invalid address code enum
            "AddressCode:6=@#$%!!",          # Row 6: special chars in address code
            "--dev2",
            "--rows", "10",
            "--test-name", "test_address_location_formatting_rules"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
