#!/bin/bash

set -e

echo "🚀 Starting Airflow with LocalExecutor and PostgreSQL..."

# Step 1: Build and start the stack
docker compose up --build -d

# Step 2: Wait for a few seconds to let services come up
echo "Waiting for Airflow webserver and scheduler to start..."
sleep 15

# Step 3: Show logs for debugging if needed
echo "Checking Airflow webserver logs..."
docker compose logs airflow | grep "Listening at: http://0.0.0.0:8080"

# Step 4: List DAGs to verify Airflow is running correctly
echo "Listing DAGs registered with Airflow:"
docker compose exec airflow airflow dags list

# Optional: Tail logs for live debugging
echo "You can tail logs using:"
echo "    docker compose logs -f airflow"

echo "Done. Visit Airflow UI at: http://localhost:8080"
echo "Login: admin | Password: admin"
