import sys
import os
import subprocess

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    generator_path = os.path.join(base_dir, "newaugsver_clean.py")
    pipeline_path = os.path.join(base_dir, "DM_bankfile_validate_pipeline.py")

    # Step 1: Generate data
    gen_command = [sys.executable, generator_path, "--rows", "25", "--output-dir", os.path.join(base_dir, "test_output"), "--output", "test_data_1"]
    gen_result = subprocess.run(gen_command)
    if gen_result.returncode != 0:
        print("Data generation failed.")
        sys.exit(gen_result.returncode)

    # Step 2: Run the pipeline script
    pipe_command = [sys.executable, pipeline_path, "--dev2", "--rows", "25"]
    pipe_result = subprocess.run(pipe_command)
    sys.exit(pipe_result.returncode)
