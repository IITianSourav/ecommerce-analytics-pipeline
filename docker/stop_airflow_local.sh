#!/bin/bash

echo "Stopping Airflow stack..."

# Step 1: Stop and remove all services
docker compose down

# Step 2: Ask user if volumes should be removed too
read -p "Do you also want to remove Docker volumes? This will delete your PostgreSQL data. (y/n): " confirm

if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
  docker volume rm ecommerce-analytics-pipeline_postgres-db-volume || true
  echo "Volumes removed."
else
  echo "Volumes retained."
fi

echo "Stack stopped. To restart, run: ./run_airflow_local.sh"
