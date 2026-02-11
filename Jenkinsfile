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
        // Only hardcode what's NOT in the Python script
        TARGET_ROLE = "arn:aws:iam::448049811908:role/mtf-pm-dev-jenkins-execution-role"
        AWS_REGION = "us-east-1"
    }
    
    stages {
        stage('Verify Initial Identity') {
            steps {
                container('awscli') {
                    echo 'Checking initial AWS identity from service account...'
                    sh 'aws sts get-caller-identity'
                }
            }
        }
        
        stage('Assume Target Role') {
            steps {
                container('awscli') {
                    script {
                        echo "Assuming role: ${env.TARGET_ROLE}"
                        
                        sh '''
                        # Assume the role with S3 permissions
                        aws sts assume-role \
                          --role-arn ${TARGET_ROLE} \
                          --role-session-name jenkins-test-${BUILD_NUMBER} \
                          --output text \
                          --query Credentials \
                          > ${WORKSPACE}/.role-creds.txt
                        
                        # Extract credentials
                        export AWS_ACCESS_KEY_ID=$(cut -f1 ${WORKSPACE}/.role-creds.txt)
                        export AWS_SECRET_ACCESS_KEY=$(cut -f3 ${WORKSPACE}/.role-creds.txt)
                        export AWS_SESSION_TOKEN=$(cut -f4 ${WORKSPACE}/.role-creds.txt)
                        
                        # Save to workspace for use in other containers
                        cat > ${WORKSPACE}/.aws-env-vars.sh <<EOF
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}
export AWS_DEFAULT_REGION=${AWS_REGION}
unset AWS_WEB_IDENTITY_TOKEN_FILE
unset AWS_ROLE_ARN
EOF
                        
                        echo "Successfully assumed role"
                        
                        # Verify the assumed role
                        . ${WORKSPACE}/.aws-env-vars.sh
                        aws sts get-caller-identity
                        '''
                    }
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
                    echo 'Testing S3 access with assumed role...'
                    sh '''
                        # Source the assumed role credentials
                        . ${WORKSPACE}/.aws-env-vars.sh
                        
                        echo "=== Current AWS Identity ==="
                        aws sts get-caller-identity
                        
                        echo ""
                        echo "=== Testing S3 Access ==="
                        # The Python script has BUCKET defined, so we don't need to hardcode it here
                        # Just verify we can list S3 buckets
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
                        
                        # Source AWS credentials for Python tests
                        . ${WORKSPACE}/.aws-env-vars.sh
                        
                        # Create allure-results directory with proper permissions
                        mkdir -p ${WORKSPACE}/allure-results
                        chmod 777 ${WORKSPACE}/allure-results
                        
                        # Run pytest with Allure results - runs all 3 tests (valid, invalid, SQL connectivity)
                        python3 -m pytest tests/test_etl_allure.py \
                            --alluredir=${WORKSPACE}/allure-results \
                            -v \
                            --tb=short
                        
                        # Fix permissions on allure results for jenkins user
                        chmod -R 777 ${WORKSPACE}/allure-results
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
                        
                        # Source AWS credentials and run tests
                        . ${WORKSPACE}/.aws-env-vars.sh
                        python3 tests/run_sql_test.py
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
            
            container('python') {
                sh '''
                    # Cleanup sensitive files
                    rm -f ${WORKSPACE}/.aws-env-vars.sh ${WORKSPACE}/.role-creds.txt
                    echo "Cleaned up temporary credential files"
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
