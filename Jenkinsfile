pipeline {
    agent any

    options {
        skipDefaultCheckout(true)
    }

    stages {

        stage('Checkout') {
            steps {
                echo 'Checking out source code from GitHub...'
                checkout scm
            }
        }

        stage('Verify Repository') {
            steps {
                echo 'Repository successfully checked out.'
                sh '''
                    echo "Current directory:"
                    pwd

                    echo "Repository files:"
                    ls -la
                '''
            }
        }

    }

    post {
        success {
            echo 'CI pipeline completed successfully.'
        }

        failure {
            echo 'CI pipeline failed.'
        }
    }
}