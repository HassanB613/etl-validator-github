"""
Pytest configuration and hooks for checkpoint/restart behavior.
Handles skipping completed tests and monitoring credential expiry.
"""

import pytest
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path so we can import checkpoint_manager
sys.path.insert(0, str(Path(__file__).parent.parent))

from checkpoint_manager import load_checkpoint, save_checkpoint

# Load completed tests from checkpoint at session start
COMPLETED_TESTS = load_checkpoint()

# Track passed tests in this session
PASSED_TESTS_THIS_SESSION = []

def pytest_configure(config):
    """
    Called after command line options have been parsed.
    Displays checkpoint status.
    """
    if COMPLETED_TESTS:
        print(f"\n📂 RESUMING FROM CHECKPOINT")
        print(f"   Skipping {len(COMPLETED_TESTS)} previously completed tests")
        print(f"   Running remaining tests...\n")
    else:
        print(f"\n🆕 FRESH RUN")
        print(f"   No checkpoint found - running all tests\n")

def pytest_collection_modifyitems(config, items):
    """
    Called after test collection.
    Marks tests as skipped if they're in the completed list.
    """
    for item in items:
        # Get test full path
        test_full_name = item.nodeid
        
        # Check against completed tests
        for completed in COMPLETED_TESTS:
            if completed in test_full_name:
                item.add_marker(pytest.mark.skip(reason="Already completed in previous run"))
                break

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_makereport(item, call):
    """
    Called after each test phase (setup, call, teardown).
    Tracks test results.
    """
    if call.when == "call":
        if call.excinfo is None:  # Test passed
            PASSED_TESTS_THIS_SESSION.append(item.nodeid)

def pytest_sessionfinish(session, exitstatus):
    """
    Called after the test session has ended.
    Saves checkpoint if needed.
    """
    total_passed = len(PASSED_TESTS_THIS_SESSION) + len(COMPLETED_TESTS)
    
    print(f"\n{'='*60}")
    print(f"📊 TEST SESSION SUMMARY")
    print(f"{'='*60}")
    print(f"Passed in this session: {len(PASSED_TESTS_THIS_SESSION)}")
    print(f"Skipped (from checkpoint): {len(COMPLETED_TESTS)}")
    print(f"Total passed so far: {total_passed}")
    print(f"{'='*60}\n")
    
    # If tests failed due to credential expiry, save checkpoint
    if exitstatus != 0 and PASSED_TESTS_THIS_SESSION:
        all_passed = COMPLETED_TESTS + PASSED_TESTS_THIS_SESSION
        save_checkpoint(all_passed, reason="credential_expiry_detected")
        print(f"💾 Checkpoint saved with {len(all_passed)} total completed tests")
        print(f"🔁 Jenkins will restart the job with fresh credentials\n")
