"""
Checkpoint Manager for Long-Running Test Suites
Saves progress when credentials are expiring to allow job restart with fresh credentials.
"""

import json
import os
from datetime import datetime
from pathlib import Path

CHECKPOINT_FILE = ".test_checkpoint.json"

def save_checkpoint(completed_tests: list, reason: str = "credential_expiry") -> None:
    """
    Save list of completed tests to checkpoint file.
    
    Args:
        completed_tests: List of test names that have passed
        reason: Reason for checkpoint (e.g., 'credential_expiry', 'test_complete')
    """
    checkpoint_data = {
        "timestamp": datetime.now().isoformat(),
        "reason": reason,
        "completed_tests": completed_tests,
        "total_completed": len(completed_tests)
    }
    
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    
    print(f"💾 Checkpoint saved: {len(completed_tests)} tests completed")
    print(f"   Reason: {reason}")
    print(f"   File: {CHECKPOINT_FILE}")

def load_checkpoint() -> list:
    """
    Load list of previously completed tests from checkpoint file.
    
    Returns:
        List of completed test names, or empty list if no checkpoint exists
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return []
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint_data = json.load(f)
        
        completed = checkpoint_data.get("completed_tests", [])
        print(f"📂 Loaded checkpoint: {len(completed)} tests already completed")
        print(f"   Saved at: {checkpoint_data.get('timestamp')}")
        print(f"   Reason: {checkpoint_data.get('reason')}")
        return completed
    except Exception as e:
        print(f"⚠️ Could not load checkpoint: {e}")
        return []

def clear_checkpoint() -> None:
    """Remove checkpoint file when all tests are complete."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("🗑️ Checkpoint cleared - all tests completed")

def get_checkpoint_display() -> str:
    """Get human-readable checkpoint status."""
    if not os.path.exists(CHECKPOINT_FILE):
        return "No checkpoint (fresh run)"
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
        return f"Resuming from checkpoint: {data['total_completed']} tests completed"
    except:
        return "Checkpoint exists but unreadable"

def should_skip_test(test_name: str, completed_tests: list) -> bool:
    """
    Check if a test should be skipped (already completed).
    
    Args:
        test_name: Name/path of test to check
        completed_tests: List of completed test names
    
    Returns:
        True if test should be skipped, False if it should run
    """
    return test_name in completed_tests
