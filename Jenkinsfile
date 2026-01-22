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
                bat 'python tests/test_run_1.py'
                echo 'Running test_run_2.py...'
                bat 'python tests/test_run_2.py'
                echo 'Running test_run_3.py...'
                bat 'python tests/test_run_3.py'
            }
        }
        stage('SQL Test') {
            steps {
                echo 'Running SQL tests...'
                // Example: Run SQL scripts (customize as needed)
                // bat 'sqlcmd -S <server> -d <db> -U <user> -P <password> -i sql/test_script.sql'
            }
        }
    }
    post {
        always {
            echo 'Pipeline finished.'
        }
    }
}
