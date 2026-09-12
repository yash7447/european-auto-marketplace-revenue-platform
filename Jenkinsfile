pipeline {

    agent any

    options {
        timestamps()
        disableConcurrentBuilds()

        timeout(
            time: 30,
            unit: 'MINUTES'
        )
    }

    environment {

        COMPOSE_PROJECT_NAME =
            'european-auto-marketplace-revenue-platform'

        AWS_PROFILE =
            'auto-marketplace'

        AWS_DEFAULT_REGION =
            'us-east-1'

        ATHENA_BUCKET =
            'european-auto-marketplace-data-yash-2026-01'
    }


    stages {


        // ====================================================
        // 1. CHECKOUT
        // ====================================================

        stage('Checkout') {

            steps {

                echo 'Checking out latest code...'

                checkout scm

                sh '''
                    echo "Commit:"
                    git rev-parse --short HEAD

                    echo "Branch:"
                    git branch --show-current || true
                '''
            }
        }


        // ====================================================
        // 2. PYTHON SYNTAX VALIDATION
        // ====================================================

        stage('Python Syntax Validation') {

            steps {

                echo 'Validating Python syntax...'

                sh '''
                    python3 -m compileall -q \
                      infrastructure \
                      src/platform \
                      airflow/dags

                    echo "Python syntax validation passed."
                '''
            }
        }


        // ====================================================
        // 3. DOCKER COMPOSE VALIDATION
        // ====================================================

        stage('Docker Compose Validation') {

            steps {

                echo 'Validating Docker Compose configuration...'

                sh '''
                    docker compose \
                      -p "$COMPOSE_PROJECT_NAME" \
                      -f docker-compose.sources.yml \
                      config -q

                    docker compose \
                      -f docker-compose.airflow.yml \
                      config -q

                    echo "Docker Compose validation passed."
                '''
            }
        }


        // ====================================================
        // 4. BUILD SOURCE SYSTEM IMAGES
        // ====================================================

        stage('Docker Image Build') {

            steps {

                echo 'Building source system images...'

                sh '''
                    docker compose \
                      -p "$COMPOSE_PROJECT_NAME" \
                      -f docker-compose.sources.yml \
                      build \
                      commercial-api \
                      marketplace-seeder \
                      pricing-api \
                      engagement-generator \
                      sales-target-generator

                    echo "Docker image build passed."
                '''
            }
        }


        // ====================================================
        // 5. START CORE SOURCE SYSTEMS
        // ====================================================

        stage('Start Core Source Systems') {

            steps {

                echo 'Starting source systems using latest images...'

                sh '''
                    docker compose \
                      -p "$COMPOSE_PROJECT_NAME" \
                      -f docker-compose.sources.yml \
                      up -d \
                      --force-recreate \
                      commercial-api \
                      marketplace-db \
                      pricing-api
                '''
            }
        }


        // ====================================================
        // 6. WAIT FOR SERVICES
        // ====================================================

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


                    echo "Waiting for Marketplace PostgreSQL..."

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
                            echo "Marketplace PostgreSQL failed to start."
                            exit 1
                        fi

                        sleep 2
                    done
                '''
            }
        }


        // ====================================================
        // 7. API INTEGRATION TESTS
        // ====================================================

        stage('API Integration Tests') {

            steps {

                echo 'Testing APIs and expected source counts...'

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
                f"{url} returned HTTP "
                f"{response.status}"
            )

        return json.loads(
            response.read().decode("utf-8")
        )


# ------------------------------------------------------------
# Commercial API
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# Pricing API
# ------------------------------------------------------------

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


        // ====================================================
        // 8. MARKETPLACE DATABASE TESTS
        // ====================================================

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


        // ====================================================
        // 9. VALIDATE ATHENA MIGRATION FRAMEWORK
        // ====================================================

        stage('Validate Athena Migrations') {

            steps {

                echo 'Validating Athena migration framework...'

                sh '''
                    echo "Checking migration directory..."

                    test -d sql/migrations


                    echo "Checking migration runner..."

                    test -f \
                      src/platform/athena_migrate.py


                    echo "Migration files:"

                    ls -1 sql/migrations


                    echo "Validating migration naming and versions..."
                '''

                sh '''
python3 - <<'PY'

from pathlib import Path
import re


migration_dir = Path(
    "sql/migrations"
)

pattern = re.compile(
    r"^V([0-9]{3,})__.+[.]sql$"
)

files = sorted(
    migration_dir.glob("*.sql")
)


if not files:

    raise SystemExit(
        "No Athena migration files found."
    )


versions = set()


for path in files:

    match = pattern.match(
        path.name
    )

    if not match:

        raise SystemExit(
            f"Invalid migration filename: "
            f"{path.name}"
        )


    version = match.group(1)


    if version in versions:

        raise SystemExit(
            f"Duplicate migration version: "
            f"V{version}"
        )


    versions.add(
        version
    )


    sql = path.read_text(
        encoding="utf-8-sig"
    ).strip()


    if not sql:

        raise SystemExit(
            f"Empty migration: "
            f"{path.name}"
        )


print(
    f"Validated {len(files)} "
    f"Athena migration files."
)

print(
    "Versions:",
    ", ".join(
        f"V{version}"
        for version in sorted(versions)
    )
)

PY
                '''

                sh '''
                    echo "Checking Python migration runner syntax..."

                    python3 -m compileall \
                      -q \
                      src/platform

                    echo "Athena migration validation passed."
                '''
            }
        }


        // ====================================================
        // 10. VERIFY AWS AUTHENTICATION
        // ====================================================

        stage('AWS Authentication Check') {

            steps {

                echo 'Verifying Jenkins AWS identity...'

                sh '''
                    echo "AWS CLI:"
                    aws --version

                    echo "AWS profile:"
                    echo "$AWS_PROFILE"

                    echo "AWS region:"
                    echo "$AWS_DEFAULT_REGION"

                    echo "AWS caller identity:"

                    aws sts get-caller-identity \
                      --profile "$AWS_PROFILE"

                    echo "AWS authentication passed."
                '''
            }
        }


        // ====================================================
        // 11. DEPLOY ATHENA / GLUE SCHEMA MIGRATIONS
        // ====================================================

        stage('Deploy Athena Migrations') {

            steps {

                echo '''
Deploying Athena schema migrations.

Already-applied migrations will be skipped.
New migrations will be executed exactly once.
Modified historical migrations will fail deployment.
'''

                sh '''
                    python3 \
                      src/platform/athena_migrate.py \
                      --profile "$AWS_PROFILE" \
                      --region "$AWS_DEFAULT_REGION" \
                      --bucket "$ATHENA_BUCKET"
                '''
            }
        }

    }


    // ========================================================
    // POST BUILD
    // ========================================================

    post {


        success {

            echo '''
============================================================
CI/CD PASSED
============================================================

[OK] Git checkout
[OK] Python syntax
[OK] Docker Compose validation
[OK] Docker builds
[OK] Core source systems started
[OK] Commercial API validation
[OK] Pricing API validation
[OK] Marketplace PostgreSQL validation
[OK] Expected source record counts
[OK] Athena migration validation
[OK] AWS authentication
[OK] Athena / Glue schema deployment

Platform code and schema are valid and deployable.
============================================================
'''
        }


        failure {

            echo '''
============================================================
CI/CD FAILED
============================================================

Inspect the first failed Jenkins stage.

Schema migration deployment stops immediately on failure.
Previously applied migrations are not modified.
============================================================
'''
        }

    }

}