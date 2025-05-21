#!/bin/bash

echo "Running Airflow Health Check..."

# Step 1: Check if Airflow webserver is responding
echo "Checking if Airflow webserver is reachable..."
STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health)

if [ "$STATUS_CODE" == "200" ]; then
  echo "Webserver is up and healthy (HTTP 200)"
else
  echo "Webserver not reachable or not healthy (HTTP $STATUS_CODE)"
fi

# Step 2: Check if the 'airflow' container is running
echo "Verifying that 'airflow' container is up..."
if docker compose ps airflow | grep -q "Up"; then
  echo "Airflow container is running"
else
  echo "Airflow container is NOT running"
  exit 1
fi

# Step 3: List DAGs
echo "Listing registered DAGs..."
docker compose exec airflow airflow dags list

# Step 4: Check scheduler heartbeat timestamp
echo "Scheduler heartbeat info:"
docker compose exec airflow airflow jobs check --job-type SchedulerJob

echo "Health check complete."
