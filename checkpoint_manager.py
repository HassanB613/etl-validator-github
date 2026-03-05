"""
Checkpoint Manager for Long-Running Test Suites
Saves progress when credentials are expiring to allow job restart with fresh credentials.
Stores checkpoints in S3 to persist across Jenkins pod restarts.
"""

import json
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# S3 configuration
S3_BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
S3_CHECKPOINT_KEY = "test-checkpoints/.test_checkpoint.json"

def _get_s3_client():
    """Get S3 client with AWS credentials from environment."""
    return boto3.client('s3', region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

def save_checkpoint(completed_tests: list, reason: str = "credential_expiry") -> None:
    """
    Save list of completed tests to S3 checkpoint.
    
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
    
    try:
        s3 = _get_s3_client()
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_CHECKPOINT_KEY,
            Body=json.dumps(checkpoint_data, indent=2),
            ContentType='application/json'
        )
        print(f"💾 Checkpoint saved to S3: {len(completed_tests)} tests completed")
        print(f"   Reason: {reason}")
        print(f"   Location: s3://{S3_BUCKET}/{S3_CHECKPOINT_KEY}")
    except Exception as e:
        print(f"⚠️ Failed to save checkpoint to S3: {e}")
        # Fallback: save locally as well for debugging
        with open('.test_checkpoint.json', 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        print(f"   Saved local backup: .test_checkpoint.json")

def load_checkpoint() -> list:
    """
    Load list of previously completed tests from S3 checkpoint.
    
    Returns:
        List of completed test names, or empty list if no checkpoint exists
    """
    try:
        s3 = _get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_CHECKPOINT_KEY)
        checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
        
        completed = checkpoint_data.get("completed_tests", [])
        print(f"📂 Loaded checkpoint from S3: {len(completed)} tests already completed")
        print(f"   Saved at: {checkpoint_data.get('timestamp')}")
        print(f"   Reason: {checkpoint_data.get('reason')}")
        print(f"   Location: s3://{S3_BUCKET}/{S3_CHECKPOINT_KEY}")
        return completed
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # No checkpoint exists - this is normal for first run
            return []
        else:
            print(f"⚠️ Could not load checkpoint from S3: {e}")
            return []
    except Exception as e:
        print(f"⚠️ Could not load checkpoint: {e}")
        return []

def clear_checkpoint() -> None:
    """Remove checkpoint from S3 when all tests are complete."""
    try:
        s3 = _get_s3_client()
        s3.delete_object(Bucket=S3_BUCKET, Key=S3_CHECKPOINT_KEY)
        print(f"🗑️ Checkpoint cleared from S3 - all tests completed")
        print(f"   Deleted: s3://{S3_BUCKET}/{S3_CHECKPOINT_KEY}")
    except Exception as e:
        print(f"⚠️ Could not clear checkpoint from S3: {e}")

def get_checkpoint_display() -> str:
    """Get human-readable checkpoint status."""
    try:
        s3 = _get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_CHECKPOINT_KEY)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return f"Resuming from checkpoint: {data['total_completed']} tests completed"
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return "No checkpoint (fresh run)"
        return "Checkpoint check failed"
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
