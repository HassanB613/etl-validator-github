# IAM Fix For Jenkins ETL Validator

Attach [mtfpm-test-automation-execution-role-policy.json](mtfpm-test-automation-execution-role-policy.json) to the IAM role used in the Jenkins assume-role step:

- Role ARN: `arn:aws:iam::448049811908:role/mtfpm-test-automation-execution-role`
- Referenced in [Jenkinsfile](../Jenkinsfile)

What this policy covers:

- S3 list/read/write/delete for the ETL prefixes used by the pipeline
- S3 read/write/delete for `test-checkpoints/*`
- Glue run inspection and start for `load-bank-file-stg-dev2`
- CloudWatch Logs read access for Glue diagnostics

What changed in Jenkins:

- [Jenkinsfile](../Jenkinsfile) now runs an AWS preflight before tests
- The preflight checks the assumed identity, S3 prefix listing, checkpoint write/read/delete, Glue `get-job-runs`, and CloudWatch Logs access
- If IAM is still incomplete, the build fails before the long pytest stage starts

If assume-role itself starts failing, that is a separate issue from this policy. In that case, verify:

- The Jenkins pod/service-account role is still allowed to call `sts:AssumeRole` on the target role
- The target role trust policy still trusts the Jenkins source role