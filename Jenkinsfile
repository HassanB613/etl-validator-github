pipeline {
        agent {
                kubernetes {
                                                                                                yaml '''
{
    "apiVersion": "v1",
    "kind": "Pod",
    "spec": {
        "serviceAccountName": "jenkins-role",
        "restartPolicy": "Never",
        "containers": [
            {
                "name": "python",
                "image": "public.ecr.aws/docker/library/python:3.9",
                "command": ["/bin/sh", "-c"],
                "args": ["cat"],
                "tty": true,
                "resources": {
                    "limits": {"cpu": "2000m", "memory": "2Gi"},
                    "requests": {"cpu": "1000m", "memory": "1Gi"}
                }
            },
            {
                "name": "awscli",
                "image": "public.ecr.aws/aws-cli/aws-cli:latest",
                "command": ["cat"],
                "tty": true
            },
            {
                "name": "java",
                "image": "public.ecr.aws/docker/library/eclipse-temurin:17-jre",
                "command": ["/bin/sh", "-c"],
                "args": ["cat"],
                "tty": true
            }
        ]
    }
}
'''
        }
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
        TARGET_ROLE = "arn:aws:iam::448049811908:role/mtfpm-test-automation-execution-role"
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
                container('awscli') {
                    echo 'Verifying AWS identity (using pod service account via IRSA)...'
                    sh 'aws sts get-caller-identity'
                }
            }
        }

        stage('Assume Target Role') {
            steps {
                container('awscli') {
                    echo 'Assuming target execution role...'
                    sh '''
                        set -e
                                                read ACCESS_KEY SECRET_KEY SESSION_TOKEN EXPIRATION <<EOF
$(aws sts assume-role \
    --role-arn "$TARGET_ROLE" \
    --role-session-name "jenkins-${BUILD_NUMBER}" \
    --duration-seconds 43200 \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken,Expiration]' \
    --output text)
EOF

                        cat > ${WORKSPACE}/.aws-env-vars.sh <<EOF
export AWS_ACCESS_KEY_ID=$ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$SECRET_KEY
export AWS_SESSION_TOKEN=$SESSION_TOKEN
export AWS_CREDENTIAL_EXPIRY=$EXPIRATION
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
EOF

                        chmod 600 ${WORKSPACE}/.aws-env-vars.sh
                        . ${WORKSPACE}/.aws-env-vars.sh
                        aws sts get-caller-identity
                    '''
                }
            }
        }
        
        stage('Build') {
            steps {
                container('python') {
                    echo 'Building...'
                    echo 'Installing Python dependencies...'
                    sh 'python3 -m pip install -r requirements.txt'
                    echo 'Installing ODBC drivers...'
                    sh '''
                        apt-get update -qq && apt-get install -y -q curl apt-transport-https gnupg
                        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
                        curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list
                        apt-get update -qq
                        ACCEPT_EULA=Y apt-get install -y -q msodbcsql17 unixodbc-dev
                    '''
                }
            }
        }
        
        stage('Verify AWS Access') {
            steps {
                container('awscli') {
                    echo 'Verifying S3, Glue, and CloudWatch Logs access using assumed target role credentials...'
                    sh '''
                        set -euo pipefail
                        . ${WORKSPACE}/.aws-env-vars.sh

                        echo "=== Current AWS Identity ==="
                        aws sts get-caller-identity

                        echo ""
                        echo "=== Testing S3 Prefix Access ==="
                        aws s3api list-objects-v2 --bucket "$ETL_BUCKET" --prefix "bankfile/ready/" --max-items 1 > /dev/null
                        aws s3api list-objects-v2 --bucket "$ETL_BUCKET" --prefix "bankfile/error/" --max-items 1 > /dev/null
                        aws s3api list-objects-v2 --bucket "$ETL_BUCKET" --prefix "bankfile/archive/" --max-items 1 > /dev/null
                        aws s3api list-objects-v2 --bucket "$ETL_BUCKET" --prefix "$CHECKPOINT_PREFIX/" --max-items 1 > /dev/null

                        echo ""
                        echo "=== Testing S3 Checkpoint Read/Write/Delete Access ==="
                        SAFE_JOB_NAME=$(printf '%s' "$JOB_NAME" | tr '/' '-')
                        PREFLIGHT_KEY="$CHECKPOINT_PREFIX/iam-preflight/${SAFE_JOB_NAME}-${BUILD_NUMBER}.txt"
                        printf 'jenkins aws preflight build=%s\n' "$BUILD_NUMBER" | aws s3 cp - "s3://$ETL_BUCKET/$PREFLIGHT_KEY"
                        aws s3 cp "s3://$ETL_BUCKET/$PREFLIGHT_KEY" - > /dev/null
                        aws s3 rm "s3://$ETL_BUCKET/$PREFLIGHT_KEY"

                        echo ""
                        echo "=== Testing Glue Read Access ==="
                        aws glue get-job-runs --job-name "$ETL_GLUE_JOB" --max-results 1 > /dev/null

                        echo ""
                        echo "=== Testing CloudWatch Logs Read Access ==="
                        aws logs describe-log-streams --log-group-name "$GLUE_OUTPUT_LOG_GROUP" --max-items 1 > /dev/null
                        aws logs describe-log-streams --log-group-name "$GLUE_ERROR_LOG_GROUP" --max-items 1 > /dev/null

                        echo ""
                        echo "AWS access preflight verified successfully"
                    '''
                }
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
                container('python') {
                    echo 'Running tests with Allure reporting...'
                    sh '''#!/bin/bash
                        set -euo pipefail

                        if [ ! -f "${WORKSPACE}/.aws-env-vars.sh" ]; then
                            echo "Missing ${WORKSPACE}/.aws-env-vars.sh; target role credentials were not prepared."
                            exit 1
                        fi

                        . ${WORKSPACE}/.aws-env-vars.sh

                        if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "${AWS_SESSION_TOKEN:-}" ]; then
                            echo "Assumed-role AWS credentials are missing in Test stage environment."
                            exit 1
                        fi

                        echo "=== Python Stage AWS Identity ==="
                        python3 - <<'PY'
import boto3
print(boto3.client('sts').get_caller_identity())
PY

                        # Create allure-results directory with proper permissions
                        mkdir -p ${WORKSPACE}/allure-results
                        chmod 777 ${WORKSPACE}/allure-results

                        # Propagate checkpoint ID into pytest process when resuming
                        if [ -n "${CHECKPOINT_ID}" ]; then
                            export CHECKPOINT_ID="${CHECKPOINT_ID}"
                            echo "Resuming from CHECKPOINT_ID=${CHECKPOINT_ID}"
                        fi
                        
                        # Run pytest - stream output live to console AND write to log file
                        python3 -m pytest tests/ \
                            --alluredir=${WORKSPACE}/allure-results \
                            -v \
                            --tb=short 2>&1 | tee ${WORKSPACE}/pytest-output.log
                        EXIT_CODE=${PIPESTATUS[0]}


                        # Detect checkpoint exit and extract checkpoint id
                        CHECKPOINT_HIT=false
                        CHECKPOINT_ID_FOUND=""
                        if grep -q "45-minute checkpoint reached" ${WORKSPACE}/pytest-output.log; then
                            CHECKPOINT_HIT=true
                            # Primary: dedicated marker line printed by conftest.py
                            CHECKPOINT_ID_FOUND=$(grep -oE 'JENKINS_CHECKPOINT_ID=[A-Za-z0-9_-]+' ${WORKSPACE}/pytest-output.log | cut -d= -f2 | tail -1)
                            # Fallback: legacy pattern from pytest.exit() message
                            if [ -z "$CHECKPOINT_ID_FOUND" ]; then
                                CHECKPOINT_ID_FOUND=$(grep -oE 'Saved checkpoint [A-Za-z0-9_-]+' ${WORKSPACE}/pytest-output.log | awk '{print $3}' | tail -1)
                            fi
                        fi
                        echo "$CHECKPOINT_HIT" > ${WORKSPACE}/checkpoint_triggered.txt
                        if [ -n "$CHECKPOINT_ID_FOUND" ]; then
                            echo "$CHECKPOINT_ID_FOUND" > ${WORKSPACE}/checkpoint_id.txt
                        fi
                        
                        # Fix permissions on allure results for jenkins user
                        chmod -R 777 ${WORKSPACE}/allure-results
                        
                        exit $EXIT_CODE
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
                container('python') {
                    echo 'Running SQL tests...'
                    sh '''
                        . ${WORKSPACE}/.aws-env-vars.sh
                        python3 run_sql_test.py
                    '''
                }
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

                    // Publish Allure Report - run inside java container
                    container('java') {
                        sh 'java -version'
                        allure([
                            includeProperties: false,
                            jdk: '',
                            properties: [],
                            reportBuildPolicy: 'ALWAYS',
                            results: [[path: 'allure-results']]
                        ])
                    }

                    // Post TestRail result with Allure HTML link and zip attachment
                    container('python') {
                        script {
                            env.TESTRAIL_STATUS = (currentBuild.currentResult == 'SUCCESS') ? '1' : '5'
                            env.TESTRAIL_RESULT_TEXT = currentBuild.currentResult
                        }
                        sh '''
                            echo "Posting TestRail result with Allure link and attachment..."

                            # Ensure dependencies are available even if Build stage was skipped
                            python3 -m pip install --quiet -r ${WORKSPACE}/requirements.txt || true

                            python3 - <<'PY'
import os
import sys

sys.path.insert(0, os.environ.get('WORKSPACE', '.'))
from DM_bankfile_validate_pipeline import report_to_testrail, TESTRAIL_TEST_ID

status = int(os.environ.get('TESTRAIL_STATUS', '1'))
result_text = os.environ.get('TESTRAIL_RESULT_TEXT', 'SUCCESS')
comment = f"Jenkins pipeline result: {result_text}"

report_to_testrail(TESTRAIL_TEST_ID, status, comment)
PY
                        '''
                    }
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
