"""
Pytest configuration and hooks for checkpoint/restart behavior.
Handles skipping completed tests and monitoring credential expiry.
"""

import pytest
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import parser as date_parser

# Add parent directory to path so we can import checkpoint_manager
sys.path.insert(0, str(Path(__file__).parent.parent))

from checkpoint_manager import load_checkpoint, save_checkpoint

# Credential expiry configuration
CREDENTIAL_BUFFER_MINUTES = 8  # Save checkpoint 8 minutes before expiry

# Load completed tests from checkpoint at session start
COMPLETED_TESTS = load_checkpoint()

# Track passed tests in this session
PASSED_TESTS_THIS_SESSION = []

def _check_credential_expiry():
    """
    Check if AWS credentials are expiring soon.
    Returns (is_expiring, time_remaining_str) tuple.
    """
    expiry_str = os.environ.get('AWS_SESSION_EXPIRY')
    if not expiry_str:
        return False, "unknown"
    
    try:
        # Parse AWS expiry timestamp (ISO 8601 format)
        expiry_time = date_parser.isoparse(expiry_str)
        current_time = datetime.now(expiry_time.tzinfo)
        
        time_remaining = expiry_time - current_time
        minutes_remaining = time_remaining.total_seconds() / 60
        
        # Format time remaining for display
        hours = int(minutes_remaining // 60)
        mins = int(minutes_remaining % 60)
        time_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
        
        return minutes_remaining < CREDENTIAL_BUFFER_MINUTES, time_str
    except Exception as e:
        print(f"⚠️ Could not parse AWS_SESSION_EXPIRY: {e}")
        return False, "error"

def pytest_configure(config):
    """
    Called after command line options have been parsed.
    Displays checkpoint status and credential expiry info.
    """
    if COMPLETED_TESTS:
        print(f"\n📂 RESUMING FROM CHECKPOINT")
        print(f"   Skipping {len(COMPLETED_TESTS)} previously completed tests")
        print(f"   Running remaining tests...\n")
    else:
        print(f"\n🆕 FRESH RUN")
        print(f"   No checkpoint found - running all tests\n")
    
    # Display credential expiry info
    is_expiring, time_remaining = _check_credential_expiry()
    if time_remaining != "unknown":
        print(f"⏱️ AWS credentials remaining: {time_remaining}")
        print(f"   Will save checkpoint if <{CREDENTIAL_BUFFER_MINUTES} minutes remain\n")

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

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    """
    Called before each test runs.
    Checks if credentials are expiring soon and saves checkpoint if needed.
    """
    # Check credential expiry BEFORE running test
    is_expiring, time_remaining = _check_credential_expiry()
    
    if is_expiring and (PASSED_TESTS_THIS_SESSION or COMPLETED_TESTS):
        # Save checkpoint NOW while credentials are still valid
        all_passed = COMPLETED_TESTS + PASSED_TESTS_THIS_SESSION
        print(f"\n⏰ PROACTIVE CHECKPOINT SAVE")
        print(f"{'='*60}")
        print(f"AWS credentials expiring in {time_remaining}")
        print(f"Saving checkpoint with {len(all_passed)} completed tests...")
        
        save_checkpoint(all_passed, reason="proactive_credential_expiry")
        
        print(f"✅ Checkpoint saved successfully to S3")
        print(f"🔁 Exiting gracefully - Jenkins will restart with fresh credentials")
        print(f"{'='*60}\n")
        
        # Exit gracefully with status code 1 to trigger Jenkins retry
        pytest.exit("Credentials expiring - checkpoint saved for resume", returncode=1)
    
    # Continue with test execution
    yield

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
