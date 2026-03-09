#!/usr/bin/env python3
"""
Checkpoint Management Utility
Helper script for viewing and managing test checkpoints.

Usage:
  python checkpoint_helper.py list            # List all active checkpoints
  python checkpoint_helper.py show <id>       # Show details of a checkpoint
  python checkpoint_helper.py delete <id>     # Delete a checkpoint
  python checkpoint_helper.py clean-all       # Delete all checkpoints
"""

import sys
import json
import boto3
from datetime import datetime
from tabulate import tabulate

S3_BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
CHECKPOINT_PREFIX = "test-checkpoints"


class CheckpointHelper:
    def __init__(self):
        self.s3_client = boto3.client("s3")
    
    def list_checkpoints(self):
        """List all active checkpoints in S3."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=CHECKPOINT_PREFIX
            )
            
            if 'Contents' not in response:
                print("No checkpoints found.")
                return
            
            checkpoints = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    checkpoint_id = obj['Key'].split('_')[1].replace('.json', '')
                    try:
                        checkpoint_obj = self.s3_client.get_object(
                            Bucket=S3_BUCKET,
                            Key=obj['Key']
                        )
                        data = json.loads(checkpoint_obj['Body'].read().decode('utf-8'))
                        checkpoints.append({
                            'ID': checkpoint_id,
                            'Run Start': data['run_start_time'][:19],
                            'Checkpoint Time': data['checkpoint_time'][:19],
                            'Elapsed (min)': f"{data['elapsed_minutes']:.1f}",
                            'Tests Completed': data['total_completed']
                        })
                    except:
                        pass
            
            if checkpoints:
                print(f"\n📋 Active Checkpoints ({len(checkpoints)} found):\n")
                print(tabulate(checkpoints, headers="keys", tablefmt="grid"))
            else:
                print("No valid checkpoints found.")
        except Exception as e:
            print(f"Error listing checkpoints: {e}")
    
    def show_checkpoint(self, checkpoint_id):
        """Show details of a specific checkpoint."""
        try:
            response = self.s3_client.get_object(
                Bucket=S3_BUCKET,
                Key=f"{CHECKPOINT_PREFIX}/checkpoint_{checkpoint_id}.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            print(f"\n📝 Checkpoint Details: {checkpoint_id}\n")
            print(f"Run Start Time:    {data['run_start_time']}")
            print(f"Checkpoint Time:   {data['checkpoint_time']}")
            print(f"Elapsed Time:      {data['elapsed_minutes']:.1f} minutes")
            print(f"Tests Completed:   {data['total_completed']}")
            
            if data['completed_tests']:
                print(f"\nCompleted Tests ({len(data['completed_tests'])}):")
                for test in sorted(data['completed_tests'])[:10]:
                    print(f"  ✅ {test}")
                if len(data['completed_tests']) > 10:
                    print(f"  ... and {len(data['completed_tests']) - 10} more")
        except Exception as e:
            print(f"Error retrieving checkpoint {checkpoint_id}: {e}")
    
    def delete_checkpoint(self, checkpoint_id):
        """Delete a specific checkpoint."""
        try:
            self.s3_client.delete_object(
                Bucket=S3_BUCKET,
                Key=f"{CHECKPOINT_PREFIX}/checkpoint_{checkpoint_id}.json"
            )
            print(f"✅ Checkpoint {checkpoint_id} deleted.")
        except Exception as e:
            print(f"Error deleting checkpoint {checkpoint_id}: {e}")
    
    def clean_all_checkpoints(self):
        """Delete all checkpoints."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=CHECKPOINT_PREFIX
            )
            
            if 'Contents' not in response:
                print("No checkpoints to clean.")
                return
            
            deleted_count = 0
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    self.s3_client.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])
                    deleted_count += 1
            
            print(f"✅ Deleted {deleted_count} checkpoint(s).")
        except Exception as e:
            print(f"Error cleaning checkpoints: {e}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    helper = CheckpointHelper()
    command = sys.argv[1]
    
    if command == "list":
        helper.list_checkpoints()
    elif command == "show" and len(sys.argv) > 2:
        helper.show_checkpoint(sys.argv[2])
    elif command == "delete" and len(sys.argv) > 2:
        helper.delete_checkpoint(sys.argv[2])
    elif command == "clean-all":
        confirm = input("Are you sure you want to delete ALL checkpoints? (yes/no): ")
        if confirm.lower() == "yes":
            helper.clean_all_checkpoints()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
