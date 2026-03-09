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
    Automatically integrate checkpoint management with each test.
    Skips tests that were already completed in a previous run.
    Marks tests as complete after successful execution.
    """
    test_name = request.node.name
    
    # Check if we've reached 45-minute mark - if so, stop test execution
    if checkpoint_manager.should_checkpoint():
        checkpoint_data = checkpoint_manager.trigger_45min_checkpoint()
        print(f"\n⚠️  45-minute checkpoint reached. Stopping test execution.")
        print(f"   Resume will continue from test: {test_name}")
        print(f"   Checkpoint ID: {checkpoint_data['checkpoint_id']}")
        pytest.exit(f"Checkpoint at 45 minutes", returncode=0)
    
    # Skip test if already completed in previous checkpoint
    if checkpoint_manager.should_skip_test(test_name):
        pytest.skip(f"Already completed in previous checkpoint (ID: {checkpoint_manager.checkpoint_id})", allow_module_level=False)
    
    # Run the test
    yield
    
    # Mark test as complete after successful execution (no exception)
    checkpoint_manager.mark_test_complete(test_name)


def pytest_configure(config):
    """
    Pytest hook called during initialization.
    """
    checkpoint_mgr = get_checkpoint_manager()
    elapsed = checkpoint_mgr.get_elapsed_minutes()
    
    print(f"\n{'=' * 60}")
    print(f"ETL Validator Test Suite with 45-Minute Checkpoint")
    print(f"{'=' * 60}")
    print(f"Checkpoint ID: {checkpoint_mgr.checkpoint_id}")
    print(f"Tests already completed: {len(checkpoint_mgr.completed_tests)}")
    if checkpoint_mgr.completed_tests:
        completed_list = sorted(list(checkpoint_mgr.completed_tests))[:5]
        print(f"Previously completed: {', '.join(completed_list)}{'...' if len(checkpoint_mgr.completed_tests) > 5 else ''}")
    print(f"Elapsed time: {elapsed:.1f} minutes")
    print(f"{'=' * 60}\n")
