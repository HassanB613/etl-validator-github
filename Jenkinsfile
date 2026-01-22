pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                echo 'Building...'
            }
        }
        stage('Test') {
            steps {
                echo 'Running test_run_1.py...'
                sh 'python3 tests/test_run_1.py'
                echo 'Running test_run_2.py...'
                sh 'python3 tests/test_run_2.py'
                echo 'Running test_run_3.py...'
                sh 'python3 tests/test_run_3.py'
            }
        }
        stage('SQL Test') {
            steps {
                echo 'Running SQL tests...'
                sh 'python3 tests/run_sql_test.py'
            }
        }
    }
    post {
        always {
            echo 'Pipeline finished.'
        }
    }
}
