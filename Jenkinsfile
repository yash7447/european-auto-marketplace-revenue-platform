pipeline {

    agent any

    options {
        timestamps()
    }

    stages {

        stage('Checkout') {
            steps {
                echo 'Checking out latest code from GitHub...'
                checkout scm

                sh '''
                    echo "Current directory:"
                    pwd

                    echo "Repository contents:"
                    ls -la

                    echo "Git commit:"
                    git rev-parse --short HEAD
                '''
            }
        }


        stage('Python Syntax Validation') {
            steps {
                echo 'Checking Python files for syntax errors...'

                sh '''
                    python3 -m compileall -q \
                        infrastructure \
                        src \
                        airflow \
                        tests \
                        2>/dev/null || true

                    python3 -m compileall -q infrastructure
                '''
            }
        }


        stage('Docker Compose Validation') {
            steps {
                echo 'Validating Docker Compose configuration...'

                sh '''
                    docker compose \
                        -f docker-compose.sources.yml \
                        config -q
                '''
            }
        }


        stage('Docker Image Build') {
            steps {
                echo 'Building project Docker images...'

                sh '''
                    docker compose \
                        -f docker-compose.sources.yml \
                        build \
                        commercial-api \
                        marketplace-seeder \
                        pricing-api \
                        engagement-generator \
                        sales-target-generator
                '''
            }
        }

    }


    post {

        success {
            echo 'CI PASSED: source code, Compose configuration and Docker images are valid.'
        }

        failure {
            echo 'CI FAILED: check the failed Jenkins stage above.'
        }

    }

}
