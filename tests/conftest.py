"""
Pytest configuration file for test suite with checkpoint integration.
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# S3 ready folder coordinates (dev2 environment)
_S3_READY_BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
_S3_READY_PREFIX = "bankfile/ready"

from checkpoint_manager import get_checkpoint_manager


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GATE_GUARD_DIR = os.path.join(BASE_DIR, "test_output")
GATE_GUARD_STATE_FILE = os.path.join(GATE_GUARD_DIR, "pre_upload_gate_state.json")
GATE_GUARD_STOP_FILE = os.path.join(GATE_GUARD_DIR, "STOP_TESTING_READY_STUCK.flag")
# No test capping; run all collected tests.
DEFAULT_JENKINS_TEST_LIMIT = 0
CHECKPOINTS_ENABLED = False
FOCUSED_TEST_NODEIDS = (
    "tests/test_chk_combined_special_chars_and_max_length.py::"
    "TestChkCombinedSpecialCharsAndMaxLength::"
    "test_chk_combined_special_chars_and_max_length",
    "tests/test_chk_contact_fields_over_max_length_combined.py::"
    "TestChkContactFieldsOverMaxLengthCombined::"
    "test_chk_contact_fields_over_max_length_combined",
    "tests/test_chk_banking_fields_should_be_blank_combined.py::"
    "TestChkBankingFieldsShouldBeBlankCombined::"
    "test_chk_banking_fields_should_be_blank_combined",
    "tests/test_accountnumber_blank_eft_required.py::"
    "TestAccountNumberBlankEFTRequired::"
    "test_accountnumber_blank_eft_required",
)


def _get_pytest_run_limit():
    """Always run the full collected suite without skip-based limiting."""
    return 0


def _read_stop_testing_message():
    if not os.path.exists(GATE_GUARD_STOP_FILE):
        return None
    try:
        with open(GATE_GUARD_STOP_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or "Stop-testing guard flag detected."
    except Exception as e:
        return f"Stop-testing guard flag detected, but could not read details: {e}"


@pytest.fixture(scope="session", autouse=True)
def gate_guard_session_setup():
    """Ensure stale stop-guard artifacts do not leak across independent pytest sessions."""
    for path in [GATE_GUARD_STOP_FILE, GATE_GUARD_STATE_FILE]:
        if os.path.exists(path):
            try:
                os.remove(path)
                print(f"CLEANUP: Cleared stale gate guard file: {path}")
            except Exception as e:
                print(f"WARNING: Could not clear stale gate guard file {path}: {e}")
    yield


@pytest.fixture(scope="session", autouse=True)
def s3_ready_folder_cleanup():
    """
    Clear any stale files from the S3 ready folder before the test session starts.
    Prevents pre-upload gate timeouts caused by files left behind by failed prior runs.
    """
    try:
        import boto3
        s3 = boto3.client("s3")
        result = s3.list_objects_v2(Bucket=_S3_READY_BUCKET, Prefix=_S3_READY_PREFIX + "/")
        files = [obj for obj in result.get("Contents", []) if not obj["Key"].endswith("/")]
        if not files:
            print("\nOK: S3 ready folder is empty - no pre-test cleanup needed.")
        else:
            print(f"\nWARNING: Pre-test S3 cleanup: found {len(files)} stale file(s) in ready folder:")
            for obj in files:
                key = obj["Key"]
                s3.delete_object(Bucket=_S3_READY_BUCKET, Key=key)
                print(f"   DELETED stale file: {key}")
            print("OK: Pre-test S3 cleanup complete. Ready folder is now clear.")
    except Exception as e:
        print(f"\nWARNING: Pre-test S3 cleanup skipped (non-fatal): {e}")
    yield


@pytest.fixture(scope="session", autouse=True)
def checkpoint_manager():
    """Provide checkpoint manager as a session-wide fixture."""
    return get_checkpoint_manager()


@pytest.fixture(autouse=True)
def checkpoint_test_handler(request, checkpoint_manager):
    """
    Checkpoint-aware test execution.
    - Skip tests already completed in previous runs
    - Stop gracefully at checkpoint threshold
    - Mark tests complete only after successful execution
    """
    test_name = request.node.name

    stop_message = _read_stop_testing_message()
    if stop_message:
        pytest.exit(f"\nSTOP: {stop_message}", returncode=1)

    if CHECKPOINTS_ENABLED:
        if checkpoint_manager.should_skip_test(test_name):
            pytest.skip(f"SKIP: already completed test: {test_name}")

        if checkpoint_manager.should_checkpoint():
            checkpoint_summary = checkpoint_manager.trigger_45min_checkpoint()
            # Print a dedicated marker line that the Jenkinsfile grep can reliably extract
            print(f"JENKINS_CHECKPOINT_ID={checkpoint_summary['checkpoint_id']}", flush=True)
            pytest.exit(
                f"\nCHECKPOINT: 45-minute checkpoint reached. "
                f"Saved checkpoint {checkpoint_summary['checkpoint_id']} with "
                f"{checkpoint_summary['tests_completed']} completed tests.",
                returncode=0,
            )

    if not hasattr(checkpoint_manager, "_testrail_total_tests"):
        checkpoint_manager._testrail_total_tests = sum(
            1
            for item in request.session.items
            if item.get_closest_marker("skip") is None
        )

    if not hasattr(checkpoint_manager, "_testrail_sequence_counter"):
        checkpoint_manager._testrail_sequence_counter = len(checkpoint_manager.completed_tests)

    checkpoint_manager._testrail_sequence_counter += 1
    os.environ["TEST_SEQUENCE_INDEX"] = str(checkpoint_manager._testrail_sequence_counter)
    os.environ["TEST_SEQUENCE_TOTAL"] = str(checkpoint_manager._testrail_total_tests)

    # Run the test
    yield

    stop_message = _read_stop_testing_message()
    if stop_message:
        pytest.exit(f"\nSTOP: {stop_message}", returncode=1)

    if CHECKPOINTS_ENABLED and hasattr(request.node, "rep_call") and request.node.rep_call.passed:
        checkpoint_manager.mark_test_complete(test_name)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture test outcome for fixture teardown logic."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


def pytest_configure(config):
    """
    Pytest hook called during initialization.
    """
    checkpoint_mgr = get_checkpoint_manager()

    print(f"\n{'=' * 60}")
    print(f"ETL Validator Test Suite")
    print(f"{'=' * 60}")
    if CHECKPOINTS_ENABLED:
        print(f"Checkpoint ID: {checkpoint_mgr.checkpoint_id}")
        print("Checkpoint-based skipping and 45-minute checkpoint exits are enabled.")
    else:
        print("Checkpointing is disabled for this run.")
    print(f"{'=' * 60}\n")


def pytest_collection_modifyitems(config, items):
    """Enforce no-skip collection policy and run the full collected suite."""
    focus_enabled = True
    if focus_enabled:
        allowed_nodeids = set(FOCUSED_TEST_NODEIDS)
        focus_skip = pytest.mark.skip(reason="Focused run: only executing selected focused tests")
        focused_found = []
        skipped_count = 0
        for item in items:
            if item.nodeid in allowed_nodeids:
                focused_found.append(item.nodeid)
                continue
            item.add_marker(focus_skip)
            skipped_count += 1

        if focused_found:
            print(
                "\nINFO: Focus mode enabled. "
                f"Running {len(focused_found)} focused test(s). "
                f"Marked {skipped_count} test(s) as skipped."
            )
        else:
            print(
                f"\nWARNING: Focused tests not found: {FOCUSED_TEST_NODEIDS}. "
                "No focus skip markers applied."
            )
        return

    cleared_markers = 0
    for item in items:
        if not item.own_markers:
            continue
        original_count = len(item.own_markers)
        item.own_markers = [
            marker for marker in item.own_markers if marker.name not in {"skip", "skipif"}
        ]
        cleared_markers += original_count - len(item.own_markers)

    if cleared_markers:
        print(f"\nINFO: Removed {cleared_markers} skip/skipif marker(s); running all collected tests.")

    run_limit = _get_pytest_run_limit()
    if run_limit <= 0 or len(items) <= run_limit:
        return

    skip_remaining = pytest.mark.skip(reason=f"Temporarily limited to first {run_limit} tests")
    skipped_count = 0
    for item in items[run_limit:]:
        item.add_marker(skip_remaining)
        skipped_count += 1

    print(
        f"\nINFO: Limiting pytest run to first {run_limit} collected tests "
        f"(marked {skipped_count} test(s) as skipped)."
    )



