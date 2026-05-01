import sys
import os
import subprocess
import allure

"""
Test Run: Contact Required/Format Rules Combined
ContactFirstName/LastName blank/numeric format, ContactPhone/Fax invalid format validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test Contact Required/Format Rules Combined')
class TestContactRequiredFormatRulesCombined:

    @allure.description("""
    Test contact field requiredness and format rules: blank validation, numeric rejection in names, special chars in phone/fax.

    Notes:
    - ContactFirstName/LastName: Required for D/P; blank invalid; no numeric chars allowed
    - ContactPhone/Fax: Required for D/P; no special chars allowed in format
    - Test uses row-isolated invalid values to validate enforcement in one run
    - D/P org context ensures contact field validation is active
    
    Test Cases:
    - Row 0: ContactFirstName blank (required for D/P)
    - Row 1: ContactLastName blank (required for D/P)
    - Row 2: ContactFirstName with numeric "John123" (format invalid)
    - Row 3: ContactLastName with numeric "Smith456" (format invalid)
    - Row 4: ContactPhone with special char "555-123-4567#" (format invalid)
    - Row 5: ContactFax with special char "555-987-6543@" (format invalid)

    Steps:
    1. Generate parquet file with 10 rows
    2. Set rows 0-5 with invalid contact field values
    3. Force D/P org context for contact field validation
    4. Upload/trigger ETL pipeline
    5. Verify pipeline detected format violations in all 6 rows
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_contact_required_format_rules(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "OrganizationCode:D",
            "OrganizationCode:0=P",
            "OrganizationCode:1=P",
            "ContactFirstName:0=",               # Row 0: blank first name (required)
            "ContactLastName:1=",                # Row 1: blank last name (required)
            "ContactFirstName:2=John123",        # Row 2: numeric in first name
            "ContactLastName:3=Smith456",        # Row 3: numeric in last name
            "ContactPhone:4=555-123-4567#",      # Row 4: special char in phone
            "ContactFax:5=555-987-6543@",        # Row 5: special char in fax
            "--dev2",
            "--rows", "10",
            "--test-name", "test_contact_required_format_rules"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
