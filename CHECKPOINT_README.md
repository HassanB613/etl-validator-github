# 45-Minute Checkpoint System

## Overview

The checkpoint system automatically handles test execution across multiple runs when AWS credentials expire after 1 hour (role-chaining limitation).

**How it works:**
1. Test suite starts and begins tracking elapsed time
2. Tests execute sequentially, marking completion after each success
3. At **45-minute mark**, the system saves a checkpoint to S3 and gracefully exits
4. On next run (with fresh credentials), tests resume from last checkpoint
5. Already-completed tests are automatically skipped
6. This process repeats until all tests pass

---

## Usage

### Normal Test Execution (With Checkpoints Enabled)

```bash
# Tests will run with checkpoint monitoring automatically
pytest tests/ -v --alluredir=allure-results

# Or with specific test selection
pytest tests/test_*.py -v --alluredir=allure-results
```

The conftest.py file automatically:
- Checks if we're past 45 minutes elapsed
- Skips tests already completed in previous run
- Marks tests as complete after successful execution
- Stops test run at 45-minute mark and exits cleanly

### Jenkins Integration

In your Jenkinsfile, the test stage should:
1. Run pytest normally (checkpoint integration is automatic)
2. If pytest exits with return code 0 and message contains "45-minute checkpoint", schedule next run
3. Collect partial Allure results from completed tests
4. On final run completion, clean up checkpoint

Example Jenkins stage:
```groovy
stage('Run Tests') {
    container('python') {
        script {
            // Run tests with checkpoint system
            int exitCode = sh(script: 'python -m pytest tests/ -v --alluredir=allure-results', returnStatus: true)
            
            if (exitCode == 0) {
                // First check if it was a checkpoint exit (clean stop at 45 min)
                sh 'python checkpoint_helper.py list'  // Show active checkpoints
                // Schedule next run or continue if tests fully passed
            }
        }
    }
}
```

---

## Checkpoint Management

### View Active Checkpoints

```bash
python checkpoint_helper.py list
```

Output example:
```
📋 Active Checkpoints (2 found):

╒════════════════════════╤═══════════════════════╤═════════════════════╤═════════════════╤══════════════════════╕
│ ID                     │ Run Start             │ Checkpoint Time     │ Elapsed (min)   │ Tests Completed      │
╞════════════════════════╪═══════════════════════╪═════════════════════╪═════════════════╪══════════════════════╡
│ a1b2c3d4               │ 2026-03-09 10:15:00   │ 2026-03-09 10:59:30 │ 44.5            │ 18                   │
│ x9y8z7w6               │ 2026-03-09 11:05:00   │ 2026-03-09 11:49:45 │ 44.8            │ 22                   │
╘════════════════════════╧═══════════════════════╧═════════════════════╧═════════════════╧══════════════════════╛
```

### Show Checkpoint Details

```bash
python checkpoint_helper.py show a1b2c3d4
```

Output example:
```
📝 Checkpoint Details: a1b2c3d4

Run Start Time:    2026-03-09T10:15:00
Checkpoint Time:   2026-03-09T10:59:30
Elapsed Time:      44.5 minutes
Tests Completed:   18

Completed Tests (18):
  ✅ test_account_number_blank
  ✅ test_account_number_invalid_length
  ✅ test_account_number_invalid_type
  ✅ test_effectivestartdate_blank
  ... and 14 more
```

### Delete a Checkpoint

```bash
# Delete specific checkpoint
python checkpoint_helper.py delete a1b2c3d4

# Delete ALL checkpoints (prompts for confirmation)
python checkpoint_helper.py clean-all
```

---

## How Checkpoints are Stored

Checkpoints are stored in S3 at:
```
s3://mtfpm-dev2-s3-mtfdmstaging-us-east-1/test-checkpoints/checkpoint_<ID>.json
```

Each checkpoint file contains:
```json
{
  "checkpoint_id": "a1b2c3d4",
  "run_start_time": "2026-03-09T10:15:00",
  "checkpoint_time": "2026-03-09T10:59:30",
  "elapsed_minutes": 44.5,
  "completed_tests": [
    "test_account_number_blank",
    "test_account_number_invalid_length",
    ...
  ],
  "total_completed": 18
}
```

---

## Checkpoint Lifecycle

### Run #1 (First attempt, 45 min limit kicked in)
- ✅ Tests 1-20 completed
- ⏰ At 45 minutes: Checkpoint saved
- ❌ Credentials expire
- 🛑 Test execution stops

**Checkpoint saved to S3:**
```
checkpoint_a1b2c3d4.json
├─ completed_tests: [test_1, test_2, ..., test_20]
└─ elapsed_minutes: 45.1
```

### Run #2 (New credentials, resuming)
- 📂 Load checkpoint from S3
- ⏭️ Skip tests 1-20 (already completed)
- ✅ Tests 21-50 execute with fresh credentials
- ✅ All tests pass
- 🗑️ Checkpoint automatically cleaned up

---

## Configuration

### Adjust Checkpoint Threshold (Optional)

Edit `checkpoint_manager.py` line 28:
```python
self.checkpoint_threshold_minutes = 45  # Change to your preferred value
```

Then restart Jenkins or next test run will use the new value.

---

## Troubleshooting

### Problem: Tests keep re-running, checkpoint not being respected

**Solution:** Checkpoint might be corrupted. Clean it:
```bash
python checkpoint_helper.py clean-all
# Or delete a specific checkpoint:
python checkpoint_helper.py delete <id>
```

### Problem: Can't see "Already completed" message in logs

**Solution:** This is logged at DEBUG level. Run with:
```bash
pytest tests/ -v --log-cli-level=DEBUG
```

### Problem: Checkpoint file not found in S3

**Cause:** AWS credentials don't have S3 write permissions to `test-checkpoints/` path

**Solution:** 
- Verify your IAM role has `s3:PutObject` and `s3:GetObject` permissions on `test-checkpoints/*`
- Check that the S3 bucket name in `checkpoint_manager.py` matches your actual bucket

### Problem: Tests completed counter doesn't increment

**Cause:** Tests might be skipped (pytest.skip()) instead of passed

**Solution:** Skipped tests don't count as "completed" (intentional design). Only passed tests increment the counter. Verify tests are actually passing, not being skipped:
```bash
pytest tests/ -v --tb=short
```

---

## Performance Impact

- **Overhead:** ~100-200ms per test for checkpoint checks (negligible)
- **S3 operations:** ~1 write per 5 minutes (checkpoint auto-save)
- **Network:** Minimal (JSON files are <1KB)

---

## Integration with EC2 Cron Run

When running tests on EC2 via cron:

1. EC2 instance has IAM role attached (credentials auto-refresh)
2. Checkpoint system still works as fallback
3. Cron script example:

```bash
#!/bin/bash
# /opt/etl-tests/run-tests.sh
cd /opt/etl-validator
/usr/bin/python3 -m pytest tests/ -v --alluredir=allure-results

# Capture exit code
EXIT_CODE=$?

# Check if it was a checkpoint stop (45 min reached)
if [ $EXIT_CODE -eq 0 ]; then
    # Check for active checkpoint
    ACTIVE_CHECKPOINT=$(/usr/bin/python3 checkpoint_helper.py list | grep "Checkpoint" | wc -l)
    if [ $ACTIVE_CHECKPOINT -gt 0 ]; then
        echo "Tests stopped at 45-min checkpoint. Next cron run will resume."
        exit 0
    else
        echo "All tests completed successfully!"
        exit 0
    fi
else
    echo "Test run failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi
```

Add to crontab to run every hour:
```
0 * * * * /opt/etl-tests/run-tests.sh >> /var/log/etl-tests.log 2>&1
```

---

## Best Practices

1. **Don't delete active checkpoints manually** unless you want to restart entire test suite
2. **Monitor checkpoint age** - if checkpoint is >3 hours old, might indicate stuck test. Check logs.
3. **Clean up after successful full runs** - manually delete checkpoint or let automated cleanup run
4. **Review S3 capacity** - checkpoints are small but clean up old ones periodically
5. **Log checkpoint info in Allure reports** for visibility:
   ```python
   import allure
   allure.attach(f"Checkpoint ID: {checkpoint_id}", "Checkpoint Info", 
                 allure.attachment_type.TEXT)
   ```

---

## Next Steps

After EC2 is provisioned and running with IAM role:
1. Same checkpoint system will work on EC2 (no changes needed)
2. EC2 will have auto-refreshing credentials, so checkpoints may not be needed
3. But checkpoint system provides extra safety layer for long-running tests

Keep checkpoint system in place even with EC2 for maximum reliability!
