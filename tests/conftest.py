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

_ACCOUNTNUMBER_ONLY_TESTS = {
    "test_accountnumber_invalid_single_digit",
    "test_accountnumber_invalid_single_digit_eft_required",
    "test_accountnumber_invalid_special_char",
}


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
                print(f"🧹 Cleared stale gate guard file: {path}")
            except Exception as e:
                print(f"⚠️ Could not clear stale gate guard file {path}: {e}")
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
            print(f"\n✅ S3 ready folder is empty — no pre-test cleanup needed.")
        else:
            print(f"\n⚠️ Pre-test S3 cleanup: found {len(files)} stale file(s) in ready folder:")
            for obj in files:
                key = obj["Key"]
                s3.delete_object(Bucket=_S3_READY_BUCKET, Key=key)
                print(f"   🗑️ Deleted stale file: {key}")
            print(f"✅ Pre-test S3 cleanup complete. Ready folder is now clear.")
    except Exception as e:
        print(f"\n⚠️ Pre-test S3 cleanup skipped (non-fatal): {e}")
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
        pytest.exit(f"\n🛑 {stop_message}", returncode=1)

    if checkpoint_manager.should_skip_test(test_name):
        pytest.skip(f"⏭️ Skipping already completed test: {test_name}")

    if checkpoint_manager.should_checkpoint():
        checkpoint_summary = checkpoint_manager.trigger_45min_checkpoint()
        # Print a dedicated marker line that the Jenkinsfile grep can reliably extract
        print(f"JENKINS_CHECKPOINT_ID={checkpoint_summary['checkpoint_id']}", flush=True)
        pytest.exit(
            f"\n⏰ 45-minute checkpoint reached. "
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
        pytest.exit(f"\n🛑 {stop_message}", returncode=1)

    if hasattr(request.node, "rep_call") and request.node.rep_call.passed:
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
    print(f"ETL Validator Test Suite (Checkpoint Enabled)")
    print(f"{'=' * 60}")
    print(f"Checkpoint ID: {checkpoint_mgr.checkpoint_id}")
    print("Checkpoint-based skipping and 45-minute checkpoint exits are enabled.")
    print(f"{'=' * 60}\n")


def pytest_collection_modifyitems(config, items):
    """Temporarily limit execution to the three account-number tests."""
    skip_non_accountnumber = pytest.mark.skip(
        reason="Temporarily skipping: only running 3 account-number tests."
    )

    for item in items:
        if item.name not in _ACCOUNTNUMBER_ONLY_TESTS:
            item.add_marker(skip_non_accountnumber)
