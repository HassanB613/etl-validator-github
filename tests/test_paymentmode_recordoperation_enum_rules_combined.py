import sys
import os
import subprocess
import allure

"""
Test Run: PaymentMode/RecordOperation Enum Invalid Rules Combined
Invalid PaymentMode and RecordOperation enum value validation.
"""


@allure.feature('ETL Validation')
@allure.story('Invalid Data Handling')
@allure.title('Test PaymentMode/RecordOperation Enum Invalid Rules Combined')
class TestPaymentModeRecordOperationEnumRulesCombined:

    @allure.description("""
    Test PaymentMode and RecordOperation enum field validation for invalid values.

    Notes:
    - PaymentMode valid enum: CHK, EFT only (no numeric, no invalid codes like POP)
    - RecordOperation valid enum: A (Add), C (Change), D (Delete) only (no Z, no special chars)
    - Test uses row-isolated invalid values to validate enforcement in one run
    
    Test Cases:
    - Row 0: PaymentMode invalid numeric "123"
    - Row 1: PaymentMode invalid enum "POP" (not CHK/EFT)
    - Row 2: RecordOperation invalid special char "$"
    - Row 3: RecordOperation invalid enum "Z" (not A/C/D)

    Steps:
    1. Generate parquet file with 10 rows
    2. Set rows 0-3 with invalid enum values
    3. Upload/trigger ETL pipeline
    4. Verify pipeline detected all 4 invalid enum violations
    """)
    @allure.severity(allure.severity_level.CRITICAL)
    def test_paymentmode_recordoperation_enum_rules(self):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

        pipe_command = [
            sys.executable, pipeline_path,
            "--invalid-values",
            "PaymentMode:0=123",           # Row 0: numeric payment mode
            "PaymentMode:1=POP",           # Row 1: invalid enum
            "RecordOperation:2=$",         # Row 2: special char in operation
            "RecordOperation:3=Z",         # Row 3: invalid operation enum
            "--dev2",
            "--rows", "10",
            "--test-name", "test_paymentmode_recordoperation_enum_rules"
        ]

        result = subprocess.run(pipe_command, cwd=base_dir)
        assert result.returncode == 0, "Pipeline execution failed"
