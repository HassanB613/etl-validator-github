pipeline {
    agent {
        kubernetes {
            yaml '''
apiVersion: v1
kind: Pod
spec:
  serviceAccountName: jenkins-role
  restartPolicy: Never
  containers:
  - name: python
    image: python:3.9
    command: ["/bin/sh", "-c"]
    args: ["cat"]
    tty: true
    resources:
      limits:
        cpu: 500m
        memory: 1Gi
  - name: awscli
    image: amazon/aws-cli:latest
    command: ["cat"]
    tty: true
  - name: java
    image: eclipse-temurin:17-jre
    command: ["/bin/sh", "-c"]
    args: ["cat"]
    tty: true
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
    }

    environment {
        AWS_DEFAULT_REGION = "us-east-1"
        TARGET_ROLE = "arn:aws:iam::448049811908:role/mtfpm-test-automation-execution-role"
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
    --duration-seconds 3600 \
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
                }
            }
        }
        
        stage('Verify AWS Access') {
            steps {
                container('awscli') {
                    echo 'Verifying S3 access using assumed target role credentials...'
                    sh '''
                        . ${WORKSPACE}/.aws-env-vars.sh

                        echo "=== Current AWS Identity ==="
                        aws sts get-caller-identity

                        echo ""
                        echo "=== Testing S3 Access ==="
                        aws s3 ls | head -5

                        echo ""
                        echo "AWS access verified successfully"
                    '''
                }
            }
        }
        
        stage('Test') {
            steps {
                container('python') {
                    echo 'Running tests with Allure reporting...'
                    sh '''
                        . ${WORKSPACE}/.aws-env-vars.sh

                        # Install ODBC drivers (required for pyodbc/database validation)
                        apt-get update && apt-get install -y curl apt-transport-https gnupg
                        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
                        curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list
                        apt-get update
                        ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

                        # Create allure-results directory with proper permissions
                        mkdir -p ${WORKSPACE}/allure-results
                        chmod 777 ${WORKSPACE}/allure-results

                        # Propagate checkpoint ID into pytest process when resuming
                        if [ -n "${CHECKPOINT_ID}" ]; then
                            export CHECKPOINT_ID="${CHECKPOINT_ID}"
                            echo "Resuming from CHECKPOINT_ID=${CHECKPOINT_ID}"
                        fi
                        
                        # Run pytest with Allure results
                        python3 -m pytest tests/ \
                            --alluredir=${WORKSPACE}/allure-results \
                            -v \
                            --tb=short > ${WORKSPACE}/pytest-output.log 2>&1 || EXIT_CODE=$?

                        # Print pytest output back into Jenkins console
                        cat ${WORKSPACE}/pytest-output.log
                        
                        # Handle exit codes
                        if [ -z "$EXIT_CODE" ]; then
                            EXIT_CODE=0
                        fi

                        # Detect checkpoint exit and extract checkpoint id
                        CHECKPOINT_HIT=false
                        CHECKPOINT_ID_FOUND=""
                        if grep -q "45-minute checkpoint reached" ${WORKSPACE}/pytest-output.log; then
                            CHECKPOINT_HIT=true
                            CHECKPOINT_ID_FOUND=$(sed -n 's/.*Saved checkpoint \([A-Za-z0-9_-]*\) with.*/\1/p' ${WORKSPACE}/pytest-output.log | tail -1)
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
                            env.NEXT_CHECKPOINT_ID = checkpointId ?: ''
                            currentBuild.description = "Checkpoint pause: ${env.NEXT_CHECKPOINT_ID} | resume ${params.RESUME_COUNT ?: '0'}/${env.MAX_RESUME_COUNT}"
                            echo "Checkpoint pause detected. Next run will resume with CHECKPOINT_ID=${env.NEXT_CHECKPOINT_ID}"
                        }
                    }
                }
            }
        }

        stage('Queue Resume Run') {
            when {
                expression { env.CHECKPOINT_TRIGGERED == 'true' }
            }
            steps {
                script {
                    if (!env.NEXT_CHECKPOINT_ID?.trim()) {
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
                    currentBuild.description = "Checkpoint queued: ${env.NEXT_CHECKPOINT_ID} | next ${nextResumeCount}/${maxResumeCount}"

                    echo "Queuing resume build with CHECKPOINT_ID=${env.NEXT_CHECKPOINT_ID} (resume ${nextResumeCount}/${maxResumeCount})"
                    build(
                        job: env.JOB_NAME,
                        wait: false,
                        parameters: [
                            string(name: 'CHECKPOINT_ID', value: env.NEXT_CHECKPOINT_ID),
                            string(name: 'RESUME_COUNT', value: nextResumeCount.toString())
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

                        # Skip ODBC install if already installed from Test stage
                        if command -v odbcinst > /dev/null 2>&1; then
                            echo "ODBC drivers already installed, skipping installation"
                        else
                            apt-get update && apt-get install -y curl apt-transport-https gnupg
                            curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --batch --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
                            curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list
                            apt-get update
                            ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev
                        fi
                        
                        python3 run_sql_test.py
                    '''
                }
            }
        }
    }
    
    post {
        always {
            echo 'Pipeline finished.'
            
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
            echo 'Pipeline failed. Check logs above.'
        }
    }
}
