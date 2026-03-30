pipeline {
    agent {
        label 'smoke-test-ec2-agent'
    }
    
    // Allure tool configured in Jenkins Global Tool Configuration
    tools {
        allure 'allure'
    }

    parameters {
        string(name: 'CHECKPOINT_ID', defaultValue: '', description: 'Checkpoint ID used to resume paused test runs')
        string(name: 'RESUME_COUNT', defaultValue: '0', description: 'Internal counter for checkpoint auto-resume attempts')
        string(name: 'PREVIOUS_BUILD_NUMBER', defaultValue: '', description: 'Previous build number for Allure result aggregation across checkpoint resumes')
    }

    environment {
        AWS_DEFAULT_REGION = "us-east-1"
        ETL_BUCKET = "mtfpm-dev2-s3-mtfdmstaging-us-east-1"
        ETL_GLUE_JOB = "load-bank-file-stg-dev2"
        CHECKPOINT_PREFIX = "test-checkpoints"
        GLUE_OUTPUT_LOG_GROUP = "/aws-glue/jobs/output"
        GLUE_ERROR_LOG_GROUP = "/aws-glue/jobs/error"
        CHECKPOINT_TRIGGERED = "false"
        NEXT_CHECKPOINT_ID = ""
        MAX_RESUME_COUNT = "6"
    }
    
    stages {
        stage('Verify Initial Identity') {
            steps {
                echo 'Verifying AWS identity from EC2 instance profile...'
                powershell '''
                    $ErrorActionPreference = "Stop"

                    $callerArn = aws sts get-caller-identity --query Arn --output text
                    $refreshRaw = "$env:ETL_USE_ASSUME_ROLE_REFRESH"
                    if ([string]::IsNullOrWhiteSpace($refreshRaw)) { $refreshRaw = "unset" }
                    $refreshEnabled = @("1", "true", "yes") -contains $refreshRaw.ToLowerInvariant()

                    Write-Host "[PREFLIGHT] Node=$env:NODE_NAME | CallerArn=$callerArn | AssumeRoleRefreshEnabled=$refreshEnabled (ETL_USE_ASSUME_ROLE_REFRESH=$refreshRaw)"
                '''
            }
        }
        
        stage('Build') {
            steps {
                echo 'Building on Windows EC2 agent...'
                echo 'Installing Python dependencies...'
                bat 'python -m pip install -r requirements.txt'
                powershell '''
                    $ErrorActionPreference = "Stop"
                    $driver = Get-OdbcDriver | Where-Object { $_.Name -eq "ODBC Driver 17 for SQL Server" }
                    if ($null -eq $driver) {
                        Write-Error "ODBC Driver 17 for SQL Server is not installed on the EC2 agent."
                    }
                    Write-Host "ODBC Driver 17 for SQL Server is available."
                '''
            }
        }
        
        stage('Verify AWS Access') {
            steps {
                echo 'Verifying S3, Glue, and CloudWatch Logs access using EC2 instance profile credentials...'
                powershell '''
                    $ErrorActionPreference = "Stop"

                    Write-Host "=== Current AWS Identity ==="
                    aws sts get-caller-identity

                    Write-Host ""
                    Write-Host "=== Testing S3 Prefix Access ==="
                    aws s3api list-objects-v2 --bucket "$env:ETL_BUCKET" --prefix "bankfile/ready/" --max-items 1 | Out-Null
                    aws s3api list-objects-v2 --bucket "$env:ETL_BUCKET" --prefix "bankfile/error/" --max-items 1 | Out-Null
                    aws s3api list-objects-v2 --bucket "$env:ETL_BUCKET" --prefix "bankfile/archive/" --max-items 1 | Out-Null
                    aws s3api list-objects-v2 --bucket "$env:ETL_BUCKET" --prefix "$env:CHECKPOINT_PREFIX/" --max-items 1 | Out-Null

                    Write-Host ""
                    Write-Host "=== Testing S3 Checkpoint Read/Write/Delete Access ==="
                    $safeJobName = "$env:JOB_NAME" -replace "/", "-"
                    $preflightKey = "$env:CHECKPOINT_PREFIX/iam-preflight/$safeJobName-$env:BUILD_NUMBER.txt"
                    $tmpFile = Join-Path $env:WORKSPACE "aws-preflight-$env:BUILD_NUMBER.txt"
                    Set-Content -Path $tmpFile -Value "jenkins aws preflight build=$env:BUILD_NUMBER" -Encoding ascii
                    aws s3 cp "$tmpFile" "s3://$env:ETL_BUCKET/$preflightKey" | Out-Null
                    aws s3 cp "s3://$env:ETL_BUCKET/$preflightKey" "$tmpFile.verify" | Out-Null
                    aws s3 rm "s3://$env:ETL_BUCKET/$preflightKey" | Out-Null
                    Remove-Item -Force $tmpFile, "$tmpFile.verify" -ErrorAction SilentlyContinue

                    Write-Host ""
                    Write-Host "=== Testing Glue Read Access ==="
                    aws glue get-job-runs --job-name "$env:ETL_GLUE_JOB" --max-results 1 | Out-Null

                    Write-Host ""
                    Write-Host "=== Testing CloudWatch Logs Read Access ==="
                    aws logs describe-log-streams --log-group-name "$env:GLUE_OUTPUT_LOG_GROUP" --max-items 1 | Out-Null
                    aws logs describe-log-streams --log-group-name "$env:GLUE_ERROR_LOG_GROUP" --max-items 1 | Out-Null

                    Write-Host ""
                    Write-Host "AWS access preflight verified successfully"
                '''
            }
        }

        stage('Restore Prior Allure Results') {
            when {
                expression { params.CHECKPOINT_ID?.trim() }
            }
            steps {
                script {
                    def sourceBuildNumber = params.PREVIOUS_BUILD_NUMBER?.trim()
                    if (!sourceBuildNumber) {
                        try {
                            sourceBuildNumber = (env.BUILD_NUMBER.toInteger() - 1).toString()
                        } catch (Exception ignored) {
                            sourceBuildNumber = ''
                        }
                    }

                    if (!sourceBuildNumber) {
                        echo 'No previous build number available for Allure restore; continuing without historical results.'
                        return
                    }

                    echo "Attempting to restore previous Allure results from build #${sourceBuildNumber}"
                    try {
                        step([
                            $class: 'CopyArtifact',
                            projectName: env.JOB_NAME,
                            selector: [$class: 'SpecificBuildSelector', buildNumber: sourceBuildNumber],
                            filter: 'allure-results/**',
                            target: '.',
                            optional: true,
                            flatten: false,
                            fingerprintArtifacts: true
                        ])
                    } catch (Exception ex) {
                        echo "Could not restore prior Allure results (continuing): ${ex.getMessage()}"
                    }
                }
            }
        }
        
        stage('Test') {
            steps {
                echo 'Running tests with Allure reporting...'
                powershell '''
                    $ErrorActionPreference = "Stop"

                    Write-Host "=== Python Stage AWS Identity ==="
                    python -c "import boto3; print(boto3.client('sts').get_caller_identity())"

                    $allureDir = Join-Path $env:WORKSPACE "allure-results"
                    New-Item -ItemType Directory -Path $allureDir -Force | Out-Null

                    if ($env:CHECKPOINT_ID) {
                        Write-Host "Resuming from CHECKPOINT_ID=$env:CHECKPOINT_ID"
                    }

                    $pytestLog = Join-Path $env:WORKSPACE "pytest-output.log"
                    python -m pytest tests/ --alluredir="$allureDir" -v --tb=short 2>&1 | Tee-Object -FilePath $pytestLog
                    $exitCode = $LASTEXITCODE

                    $checkpointHit = Select-String -Path $pytestLog -Pattern "45-minute checkpoint reached" -Quiet
                    Set-Content -Path (Join-Path $env:WORKSPACE "checkpoint_triggered.txt") -Value ($checkpointHit.ToString().ToLowerInvariant()) -Encoding ascii

                    if ($checkpointHit) {
                        $checkpointId = ""
                        $marker = Select-String -Path $pytestLog -Pattern "JENKINS_CHECKPOINT_ID=([A-Za-z0-9_-]+)" -AllMatches
                        if ($marker.Matches.Count -gt 0) {
                            $checkpointId = $marker.Matches[$marker.Matches.Count - 1].Groups[1].Value
                        }
                        if (-not $checkpointId) {
                            $legacy = Select-String -Path $pytestLog -Pattern "Saved checkpoint ([A-Za-z0-9_-]+)" -AllMatches
                            if ($legacy.Matches.Count -gt 0) {
                                $checkpointId = $legacy.Matches[$legacy.Matches.Count - 1].Groups[1].Value
                            }
                        }
                        if ($checkpointId) {
                            Set-Content -Path (Join-Path $env:WORKSPACE "checkpoint_id.txt") -Value $checkpointId -Encoding ascii
                        }
                    }

                    exit $exitCode
                '''

                script {
                    def checkpointTriggered = fileExists("${env.WORKSPACE}/checkpoint_triggered.txt") &&
                        readFile("${env.WORKSPACE}/checkpoint_triggered.txt").trim() == 'true'
                    env.CHECKPOINT_TRIGGERED = checkpointTriggered ? 'true' : 'false'

                    if (checkpointTriggered) {
                        def checkpointId = ""
                        if (fileExists("${env.WORKSPACE}/checkpoint_id.txt")) {
                            checkpointId = readFile("${env.WORKSPACE}/checkpoint_id.txt").trim()
                        }
                        if (!checkpointId?.trim()) {
                            checkpointId = params.CHECKPOINT_ID?.trim()
                        }
                        if (!checkpointId?.trim()) {
                            error("45-minute checkpoint was reached but no checkpoint ID could be extracted from pytest-output.log. Check for JENKINS_CHECKPOINT_ID= marker in the log.")
                        }
                        env.NEXT_CHECKPOINT_ID = checkpointId.trim()
                        currentBuild.description = "Checkpoint pause: ${env.NEXT_CHECKPOINT_ID} | resume ${params.RESUME_COUNT ?: '0'}/${env.MAX_RESUME_COUNT}"
                        echo "Checkpoint pause detected. Next run will resume with CHECKPOINT_ID=${checkpointId.trim()}"
                    }
                }
            }
        }

        stage('Queue Resume Run') {
            steps {
                script {
                    def checkpointTriggered = env.CHECKPOINT_TRIGGERED == 'true'
                    if (!checkpointTriggered && fileExists("${env.WORKSPACE}/checkpoint_triggered.txt")) {
                        checkpointTriggered = readFile("${env.WORKSPACE}/checkpoint_triggered.txt").trim() == 'true'
                    }

                    if (!checkpointTriggered) {
                        echo 'No checkpoint pause detected. Skipping resume queue.'
                        return
                    }

                    def checkpointId = env.NEXT_CHECKPOINT_ID?.trim()
                    if ((!checkpointId || checkpointId == 'null') && fileExists("${env.WORKSPACE}/checkpoint_id.txt")) {
                        checkpointId = readFile("${env.WORKSPACE}/checkpoint_id.txt").trim()
                    }
                    if (!checkpointId || checkpointId == 'null') {
                        checkpointId = params.CHECKPOINT_ID?.trim()
                    }
                    if (!checkpointId || checkpointId == 'null') {
                        error("Checkpoint pause detected but no CHECKPOINT_ID was found. Cannot queue resume run.")
                    }

                    int currentResumeCount = 0
                    try {
                        currentResumeCount = (params.RESUME_COUNT ?: '0').toInteger()
                    } catch (Exception ignored) {
                        currentResumeCount = 0
                    }

                    int maxResumeCount = env.MAX_RESUME_COUNT.toInteger()
                    if (currentResumeCount >= maxResumeCount) {
                        error("Max checkpoint auto-resume count reached (${currentResumeCount}/${maxResumeCount}). Stopping to prevent endless loop.")
                    }

                    int nextResumeCount = currentResumeCount + 1
                    currentBuild.description = "Checkpoint queued: ${checkpointId} | next ${nextResumeCount}/${maxResumeCount}"

                    echo "Queuing resume build with CHECKPOINT_ID=${checkpointId} (resume ${nextResumeCount}/${maxResumeCount})"
                    build(
                        job: env.JOB_NAME,
                        wait: false,
                        parameters: [
                            string(name: 'CHECKPOINT_ID', value: checkpointId),
                            string(name: 'RESUME_COUNT', value: nextResumeCount.toString()),
                            string(name: 'PREVIOUS_BUILD_NUMBER', value: env.BUILD_NUMBER)
                        ]
                    )
                }
            }
        }
        
        stage('SQL Test') {
            when {
                expression { env.CHECKPOINT_TRIGGERED != 'true' }
            }
            steps {
                echo 'Running SQL tests...'
                bat 'python run_sql_test.py'
            }
        }
    }
    
    post {
        always {
            echo 'Pipeline finished.'

            script {
                try {
                    // Persist raw Allure results so resumed builds can aggregate from prior runs
                    archiveArtifacts artifacts: 'allure-results/**', allowEmptyArchive: true

                    // Publish Allure Report
                    allure([
                        includeProperties: false,
                        jdk: '',
                        properties: [],
                        reportBuildPolicy: 'ALWAYS',
                        results: [[path: 'allure-results']]
                    ])

                    // Post TestRail result with Allure HTML link and zip attachment
                    script {
                        env.TESTRAIL_STATUS = (currentBuild.currentResult == 'SUCCESS') ? '1' : '5'
                        env.TESTRAIL_RESULT_TEXT = currentBuild.currentResult
                    }
                    powershell '''
                        $ErrorActionPreference = "Stop"
                        Write-Host "Posting TestRail result with Allure link and attachment..."

                        python -m pip install --quiet -r "$env:WORKSPACE\\requirements.txt"

                        $scriptPath = Join-Path $env:WORKSPACE "post_testrail_result.py"
                        @"
import os
import sys

sys.path.insert(0, os.environ.get('WORKSPACE', '.'))
from DM_bankfile_validate_pipeline import report_to_testrail, TESTRAIL_TEST_ID

status = int(os.environ.get('TESTRAIL_STATUS', '1'))
result_text = os.environ.get('TESTRAIL_RESULT_TEXT', 'SUCCESS')
comment = f'Jenkins pipeline result: {result_text}'

report_to_testrail(TESTRAIL_TEST_ID, status, comment)
"@ | Set-Content -Path $scriptPath -Encoding UTF8

                        python $scriptPath
                        Remove-Item -Force $scriptPath -ErrorAction SilentlyContinue
                    '''
                } catch (Exception e) {
                    echo "Skipping workspace-dependent post actions (agent/workspace unavailable): ${e.getMessage()}"
                }
            }

        }
        success {
            script {
                if (env.CHECKPOINT_TRIGGERED == 'true') {
                    echo 'Checkpoint reached. Resume run queued successfully.'
                } else {
                    echo 'All tests passed!'
                }
            }
        }
        failure {
            script {
                def readyFolderStuckDetected = false
                try {
                    if (fileExists("${env.WORKSPACE}/pytest-output.log")) {
                        def pytestLog = readFile("${env.WORKSPACE}/pytest-output.log")
                        readyFolderStuckDetected = (
                            pytestLog.contains('Detected 2 consecutive pre-upload gate failures') ||
                            pytestLog.contains('Ready folder appears stuck (Glue not clearing files)') ||
                            pytestLog.contains('Consecutive pre-upload gate failures: 2/2')
                        )
                    }
                } catch (Exception e) {
                    echo "Skipping failure log inspection (workspace unavailable): ${e.getMessage()}"
                }

                if (readyFolderStuckDetected) {
                    currentBuild.description = "FAILED: Ready folder stuck (2 gate fails)"
                    echo '============================================================'
                    echo '🛑 CRITICAL STOP CONDITION DETECTED'
                    echo 'Cause: 2 consecutive pre-upload gate failures.'
                    echo 'Meaning: Glue is likely not moving files out of ready folder.'
                    echo 'Action: Test execution was intentionally stopped to avoid noise.'
                    echo '============================================================'
                } else {
                    echo 'Pipeline failed. Check logs above.'
                }
            }
        }
    }
}
