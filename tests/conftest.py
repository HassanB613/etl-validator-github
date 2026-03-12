"""
Pytest configuration file for test suite with checkpoint integration.
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checkpoint_manager import get_checkpoint_manager


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

    if checkpoint_manager.should_skip_test(test_name):
        pytest.skip(f"⏭️ Skipping already completed test: {test_name}")

    if checkpoint_manager.should_checkpoint():
        checkpoint_summary = checkpoint_manager.trigger_45min_checkpoint()
        pytest.exit(
            f"\n⏰ 45-minute checkpoint reached. "
            f"Saved checkpoint {checkpoint_summary['checkpoint_id']} with "
            f"{checkpoint_summary['tests_completed']} completed tests.",
            returncode=0,
        )

    # Run the test
    yield

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
