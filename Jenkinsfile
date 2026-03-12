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

    environment {
        AWS_DEFAULT_REGION = "us-east-1"
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
                    echo 'Verifying S3 access using pod IRSA credentials...'
                    sh '''
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
                        # Install ODBC drivers (required for pyodbc/database validation)
                        apt-get update && apt-get install -y curl apt-transport-https gnupg
                        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
                        curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list
                        apt-get update
                        ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

                        # Create allure-results directory with proper permissions
                        mkdir -p ${WORKSPACE}/allure-results
                        chmod 777 ${WORKSPACE}/allure-results
                        
                        # Run pytest with Allure results
                        python3 -m pytest tests/ \
                            --alluredir=${WORKSPACE}/allure-results \
                            -v \
                            --tb=short || EXIT_CODE=$?
                        
                        # Handle exit codes
                        if [ -z "$EXIT_CODE" ]; then
                            EXIT_CODE=0
                        fi
                        
                        # Fix permissions on allure results for jenkins user
                        chmod -R 777 ${WORKSPACE}/allure-results
                        
                        exit $EXIT_CODE
                    '''
                }
            }
        }
        
        stage('SQL Test') {
            steps {
                container('python') {
                    echo 'Running SQL tests...'
                    sh '''
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
            echo 'All tests passed!'
        }
        failure {
            echo 'Pipeline failed. Check logs above.'
        }
    }
}
