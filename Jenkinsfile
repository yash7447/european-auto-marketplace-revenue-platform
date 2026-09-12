pipeline {

    agent any

    options {
        timestamps()
    }

    stages {

        stage('Checkout') {
            steps {

                echo 'Checking out latest code...'

                checkout scm

                sh '''
                    echo "Commit:"
                    git rev-parse --short HEAD
                '''
            }
        }


        stage('Python Syntax Validation') {
            steps {

                echo 'Validating Python syntax...'

                sh '''
                    python3 -m compileall -q infrastructure
                '''
            }
        }


        stage('Docker Compose Validation') {
            steps {

                echo 'Validating Docker Compose...'

                sh '''
                    docker compose \
                      -p european-auto-marketplace-revenue-platform \
                      -f docker-compose.sources.yml \
                      config -q
                '''
            }
        }


        stage('Docker Image Build') {
            steps {

                echo 'Building source system images...'

                sh '''
                    docker compose \
                      -p european-auto-marketplace-revenue-platform \
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


        stage('Start Core Source Systems') {
            steps {

                echo 'Starting source systems using latest images...'

                sh '''
                    docker compose \
                      -p european-auto-marketplace-revenue-platform \
                      -f docker-compose.sources.yml \
                      up -d \
                      --force-recreate \
                      commercial-api \
                      marketplace-db \
                      pricing-api
                '''
            }
        }


        stage('Wait For Services') {
            steps {

                echo 'Waiting for APIs and database...'

                sh '''
                    echo "Waiting for Commercial API..."

                    for i in $(seq 1 30); do

                        if curl -fsS \
                          http://commercial-api:8000/health \
                          > /dev/null; then

                            echo "Commercial API ready."
                            break
                        fi

                        if [ "$i" -eq 30 ]; then
                            echo "Commercial API failed to start."
                            exit 1
                        fi

                        sleep 2

                    done


                    echo "Waiting for Pricing API..."

                    for i in $(seq 1 30); do

                        if curl -fsS \
                          http://pricing-api:8000/health \
                          > /dev/null; then

                            echo "Pricing API ready."
                            break
                        fi

                        if [ "$i" -eq 30 ]; then
                            echo "Pricing API failed to start."
                            exit 1
                        fi

                        sleep 2

                    done


                    echo "Waiting for PostgreSQL..."

                    for i in $(seq 1 30); do

                        if docker exec marketplace-db \
                          pg_isready \
                          -U marketuser \
                          -d marketplace \
                          > /dev/null; then

                            echo "Marketplace PostgreSQL ready."
                            break
                        fi

                        if [ "$i" -eq 30 ]; then
                            echo "PostgreSQL failed to start."
                            exit 1
                        fi

                        sleep 2

                    done
                '''
            }
        }


        stage('API Integration Tests') {
            steps {

                echo 'Testing API responses and expected source counts...'

                sh '''
                    python3 - <<'PY'

import json
import urllib.request


def get_json(url):

    with urllib.request.urlopen(
        url,
        timeout=10
    ) as response:

        if response.status != 200:
            raise RuntimeError(
                f"{url} returned HTTP {response.status}"
            )

        return json.loads(
            response.read().decode("utf-8")
        )


commercial = get_json(
    "http://commercial-api:8000/health"
)

print(
    "Commercial API:",
    commercial
)

assert commercial["status"] == "healthy"
assert commercial["accounts"] == 5000
assert commercial["contracts"] == 8000
assert commercial["subscriptions"] == 12000
assert commercial["invoices"] == 60000


pricing = get_json(
    "http://pricing-api:8000/health"
)

print(
    "Pricing API:",
    pricing
)

assert pricing["status"] == "healthy"
assert pricing["products"] == 12
assert pricing["prices"] == 2376


print(
    "API integration tests passed."
)

PY
                '''
            }
        }


        stage('Marketplace Database Tests') {
            steps {

                echo 'Testing marketplace PostgreSQL...'

                sh '''
                    VEHICLES=$(

                        docker exec marketplace-db \
                          psql \
                          -U marketuser \
                          -d marketplace \
                          -tAc \
                          "SELECT COUNT(*) FROM vehicles;"

                    )


                    LISTINGS=$(

                        docker exec marketplace-db \
                          psql \
                          -U marketuser \
                          -d marketplace \
                          -tAc \
                          "SELECT COUNT(*) FROM listings;"

                    )


                    echo "Vehicles: $VEHICLES"
                    echo "Listings: $LISTINGS"


                    if [ "$VEHICLES" -ne 100000 ]; then

                        echo "Expected 100000 vehicles."
                        exit 1

                    fi


                    if [ "$LISTINGS" -ne 150000 ]; then

                        echo "Expected 150000 listings."
                        exit 1

                    fi


                    echo "Marketplace database tests passed."
                '''
            }
        }

    }


    post {

        success {

            echo '''
CI PASSED

? Git checkout
? Python syntax
? Docker Compose
? Docker builds
? Services started
? Commercial API
? Pricing API
? Marketplace database
? Expected source record counts
'''

        }


        failure {

            echo '''
CI FAILED

Inspect the first failed Jenkins stage.
'''

        }

    }

}
