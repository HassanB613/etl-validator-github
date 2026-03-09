"""
Checkpoint Manager for ETL Validator Pipeline
Tracks test progress and creates checkpoints at 45-minute mark to handle credential expiry.
"""

import os
import json
import boto3
from datetime import datetime, timedelta
import uuid

class CheckpointManager:
    """
    Manages checkpoints for long-running test suites.
    Saves progress at 45-minute mark to S3 for resuming after credential expiry.
    """
    
    def __init__(self, s3_checkpoint_path="s3://mtfpm-dev2-s3-mtfdmstaging-us-east-1/test-checkpoints/"):
        """
        Initialize checkpoint manager.
        
        Args:
            s3_checkpoint_path: S3 path for storing checkpoints
        """
        self.s3_bucket = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
        self.checkpoint_prefix = "test-checkpoints"
        self.s3_client = boto3.client("s3")
        self.checkpoint_id = os.environ.get("CHECKPOINT_ID", str(uuid.uuid4())[:8])
        self.run_start_time = None
        self.checkpoint_threshold_minutes = 45
        self.completed_tests = set()
        self.checkpoint_file = f"{self.checkpoint_prefix}/checkpoint_{self.checkpoint_id}.json"
        
        # Load existing checkpoint if available
        self._load_checkpoint()
    
    def _load_checkpoint(self):
        """Load checkpoint from S3 if it exists."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=self.checkpoint_file
            )
            checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
            self.run_start_time = datetime.fromisoformat(checkpoint_data['run_start_time'])
            self.completed_tests = set(checkpoint_data['completed_tests'])
            print(f"\n✅ Loaded checkpoint: {len(self.completed_tests)} tests already completed")
            print(f"   Checkpoint ID: {self.checkpoint_id}")
            print(f"   Run started at: {self.run_start_time}")
        except self.s3_client.exceptions.NoSuchKey:
            # First run, initialize timing
            self.run_start_time = datetime.now()
            self.completed_tests = set()
            print(f"\n🆕 New test run started (Checkpoint ID: {self.checkpoint_id})")
        except Exception as e:
            print(f"⚠️  Could not load checkpoint: {e}. Starting fresh.")
            self.run_start_time = datetime.now()
            self.completed_tests = set()
    
    def get_elapsed_minutes(self):
        """Get elapsed time since run start in minutes."""
        if not self.run_start_time:
            return 0
        elapsed = datetime.now() - self.run_start_time
        return elapsed.total_seconds() / 60
    
    def should_checkpoint(self):
        """
        Check if we've reached the 45-minute checkpoint threshold.
        
        Returns:
            True if 45 minutes have elapsed, False otherwise
        """
        elapsed = self.get_elapsed_minutes()
        return elapsed >= self.checkpoint_threshold_minutes
    
    def should_skip_test(self, test_name):
        """
        Check if a test should be skipped (already completed in previous run).
        
        Args:
            test_name: Name of the test to check
            
        Returns:
            True if test already completed, False if should run
        """
        return test_name in self.completed_tests
    
    def mark_test_complete(self, test_name):
        """
        Mark a test as completed and update checkpoint if needed.
        
        Args:
            test_name: Name of the test to mark as complete
        """
        self.completed_tests.add(test_name)
        elapsed = self.get_elapsed_minutes()
        
        # Save checkpoint every 5 minutes after start (for safety)
        if elapsed > 5 and elapsed % 5 < 1:  # Checkpoint every ~5 minutes
            self._save_checkpoint()
    
    def _save_checkpoint(self):
        """Save current checkpoint to S3."""
        try:
            checkpoint_data = {
                "checkpoint_id": self.checkpoint_id,
                "run_start_time": self.run_start_time.isoformat(),
                "checkpoint_time": datetime.now().isoformat(),
                "elapsed_minutes": self.get_elapsed_minutes(),
                "completed_tests": list(self.completed_tests),
                "total_completed": len(self.completed_tests)
            }
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.checkpoint_file,
                Body=json.dumps(checkpoint_data, indent=2),
                ContentType="application/json"
            )
            print(f"💾 Checkpoint saved ({len(self.completed_tests)} tests, {self.get_elapsed_minutes():.1f} min elapsed)")
        except Exception as e:
            print(f"⚠️  Failed to save checkpoint: {e}")
    
    def trigger_45min_checkpoint(self):
        """
        Trigger 45-minute checkpoint - save state and signal to stop.
        
        Returns:
            Checkpoint summary as dict
        """
        elapsed = self.get_elapsed_minutes()
        print(f"\n⏰ CHECKPOINT AT 45-MINUTE MARK ⏰")
        print(f"Elapsed time: {elapsed:.1f} minutes")
        print(f"Tests completed: {len(self.completed_tests)}")
        print(f"Checkpoint ID: {self.checkpoint_id}")
        
        self._save_checkpoint()
        
        return {
            "checkpoint_id": self.checkpoint_id,
            "elapsed_minutes": elapsed,
            "tests_completed": len(self.completed_tests),
            "completed_test_names": list(self.completed_tests)
        }
    
    def cleanup_checkpoint(self):
        """
        Delete checkpoint after successful full run completion.
        Call this after all tests pass to clean up.
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.s3_bucket,
                Key=self.checkpoint_file
            )
            print(f"🗑️  Checkpoint cleaned up after successful completion")
        except Exception as e:
            print(f"⚠️  Failed to cleanup checkpoint: {e}")


def get_checkpoint_manager():
    """Singleton-like function to get checkpoint manager instance."""
    if not hasattr(get_checkpoint_manager, '_instance'):
        get_checkpoint_manager._instance = CheckpointManager()
    return get_checkpoint_manager._instance
