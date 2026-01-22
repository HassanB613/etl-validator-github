pipeline {
    agent {
        kubernetes {
            yaml '''
apiVersion: v1
kind: Pod
spec:
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
'''
        }
    }
    stages {
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
                    sh 'python3 tests/test_run_1.py'
                    echo 'Running test_run_2.py...'
                    sh 'python3 tests/test_run_2.py'
                    echo 'Running test_run_3.py...'
                    sh 'python3 tests/test_run_3.py'
                }
            }
        }
        stage('SQL Test') {
            steps {
                container('python') {
                    echo 'Running SQL tests...'
                    sh '''
                        apt-get update && apt-get install -y unixodbc-dev
                        python3 tests/run_sql_test.py
                    '''
                }
            }
        }
    }
    post {
        always {
            echo 'Pipeline finished.'
        }
    }
}
