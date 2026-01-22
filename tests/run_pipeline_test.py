import sys
import os
import subprocess

if __name__ == "__main__":
    # Build the command to run the pipeline script with arguments
    script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "DM_bankfile_validate_pipeline.py")
    args = sys.argv[1:]
    command = [sys.executable, script_path] + args
    # Run the pipeline script with the provided arguments
    result = subprocess.run(command)
    sys.exit(result.returncode)
