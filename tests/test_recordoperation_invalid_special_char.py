import sys
import os
import subprocess

"""
Test Run 1: Special Characters in RecordOperation Column
Injects special characters ($\) into the RecordOperation column to test ETL validation.
"""

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

    # Run the pipeline with special characters injected into RecordOperation column
    # Using existing --invalid-values functionality
    pipe_command = [
        sys.executable, pipeline_path,
        "--invalid-values", "RecordOperation:$",
        "--dev2",
        "--rows", "25"
    ]
    
    print("=" * 60)
    print("TEST: Special Characters in RecordOperation Column")
    print("=" * 60)
    print(f"Injecting: $ into RecordOperation column")
    print(f"Command: {' '.join(pipe_command)}")
    print("=" * 60)
    
    pipe_result = subprocess.run(pipe_command)
    sys.exit(pipe_result.returncode)
