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
    Run tests without checkpoint-driven skipping or early checkpoint exits.
    Still marks tests as complete after successful execution for compatibility.
    """
    test_name = request.node.name

    # Run the test
    yield

    # Mark test as complete after successful execution (no exception)
    checkpoint_manager.mark_test_complete(test_name)


def pytest_configure(config):
    """
    Pytest hook called during initialization.
    """
    checkpoint_mgr = get_checkpoint_manager()

    print(f"\n{'=' * 60}")
    print(f"ETL Validator Test Suite (Checkpoint Skip Disabled)")
    print(f"{'=' * 60}")
    print(f"Checkpoint ID: {checkpoint_mgr.checkpoint_id}")
    print("Checkpoint-based skipping and 45-minute forced exits are disabled.")
    print(f"{'=' * 60}\n")
