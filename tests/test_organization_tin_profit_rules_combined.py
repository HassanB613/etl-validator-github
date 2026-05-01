import sys
import os
import subprocess
import allure

"""
Test Run: Organization/TIN/TINType/Profit Rules Combined
OrganizationCode, OrganizationIdentifier, OrganizationTIN, OrganizationTINType, and ProfitNonprofit validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test Organization/TIN/TINType/Profit Rules Combined')
class TestOrganizationTinProfitRulesCombined:

    @allure.description("""
    Test organization identification and profit status field validation rules.

    Notes:
    - OrganizationCode must be valid enum (D, P, M, R only; not H, @, etc)
    - OrganizationIdentifier: numeric values with reasonable limits; no special chars
    - OrganizationTIN: 9 digits; blank invalid for D/P; no special chars
    - OrganizationTINType: Enum (EIN or SSN only); no special chars
    - ProfitNonprofit: Enum (Y or N only); blank invalid for D/P; no special chars
    - Test uses row-isolated invalid values to validate enforcement in one run
    - D/P org context ensures required-field validation is active
    
    Test Cases (11 rows):
    - Row 0: OrganizationCode invalid enum "H" (should be D/P/M/R)
    - Row 1: OrganizationCode invalid special char "@"
    - Row 2: OrganizationIdentifier with special chars "&&&&&&&"
    - Row 3: OrganizationIdentifier with long numbers "999999999999999999"
    - Row 4: OrganizationTIN blank (required for D/P context)
    - Row 5: OrganizationTIN with special chars "!@#$"
    - Row 6: OrganizationTINType invalid enum "XXX" (should be EIN or SSN)
    - Row 7: OrganizationTINType with special chars "@#$!!!"
    - Row 8: ProfitNonprofit blank (required for D/P context)
    - Row 9: ProfitNonprofit invalid enum "FVGHJK" (should be Y or N)
    - Row 10: ProfitNonprofit with special chars "@!#$&&"

    Steps:
    1. Generate parquet file with 15 rows
    2. Set rows 0-10 with invalid organization/TIN/profit field values
    3. Force D/P org context for required-field validation
    4. Upload/trigger ETL pipeline
    5. Verify pipeline detected validation violations in all 11 rows
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_organization_tin_profit_rules(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=H",              # Row 0: invalid org code
            "OrganizationCode:1=@",              # Row 1: special char org code
            "OrganizationIdentifier:2=&&&&&&&",  # Row 2: special chars in identifier
            "OrganizationIdentifier:3=999999999999999999",  # Row 3: long numbers
            "OrganizationTIN:4=",                # Row 4: blank TIN (required for D/P)
            "OrganizationTIN:5=!@#$",            # Row 5: special chars in TIN
            "OrganizationTINType:6=XXX",         # Row 6: invalid TIN type enum
            "OrganizationTINType:7=@#$!!!",      # Row 7: special chars in TIN type
            "ProfitNonprofit:8=",                # Row 8: blank Profit (required for D/P)
            "ProfitNonprofit:9=FVGHJK",          # Row 9: invalid profit enum
            "ProfitNonprofit:10=@!#$&&",         # Row 10: special chars in profit
            "--dev2",
            "--rows", "15",
            "--test-name", "test_organization_tin_profit_rules"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
