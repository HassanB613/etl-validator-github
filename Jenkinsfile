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
'''
        }
    }
    
    environment {
        TARGET_ROLE = "arn:aws:iam::448049811908:role/mtf-pm-dev-jenkins-execution-role"
        AWS_REGION = "us-east-1"
    }
    
    stages {
        stage('Setup AWS Credentials') {
            steps {
                container('awscli') {
                    sh '''
                        aws sts assume-role \
                          --role-arn ${TARGET_ROLE} \
                          --role-session-name jenkins-${BUILD_NUMBER} \
                          --output text \
                          --query Credentials \
                          > /tmp/role-creds.txt
                        
                        export AWS_ACCESS_KEY_ID=$(cut -f1 /tmp/role-creds.txt)
                        export AWS_SECRET_ACCESS_KEY=$(cut -f3 /tmp/role-creds.txt)
                        export AWS_SESSION_TOKEN=$(cut -f4 /tmp/role-creds.txt)
                        
                        cat > /tmp/aws-env-vars.sh <<EOF
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}
export AWS_DEFAULT_REGION=${AWS_REGION}
unset AWS_WEB_IDENTITY_TOKEN_FILE
unset AWS_ROLE_ARN
EOF
                        
                        echo "AWS credentials configured"
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
        
        stage('Test') {
            steps {
                container('python') {
                    echo 'Running test_run_1.py...'
                    sh '''
                        . /tmp/aws-env-vars.sh
                        python3 tests/test_run_1.py
                    '''
                    
                    echo 'Running test_run_2.py...'
                    sh '''
                        . /tmp/aws-env-vars.sh
                        python3 tests/test_run_2.py
                    '''
                    
                    echo 'Running test_run_3.py...'
                    sh '''
                        . /tmp/aws-env-vars.sh
                        python3 tests/test_run_3.py
                    '''
                }
            }
        }
        
        stage('SQL Test') {
            steps {
                container('python') {
                    echo 'Running SQL tests...'
                    sh '''
                        apt-get update && apt-get install -y curl apt-transport-https gnupg
                        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
                        curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list
                        apt-get update
                        ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev
                        
                        . /tmp/aws-env-vars.sh
                        python3 tests/run_sql_test.py
                    '''
                }
            }
        }
    }
    
    post {
        always {
            echo 'Pipeline finished.'
            container('awscli') {
                sh 'rm -f /tmp/aws-env-vars.sh /tmp/role-creds.txt'
            }
        }
    }
}
