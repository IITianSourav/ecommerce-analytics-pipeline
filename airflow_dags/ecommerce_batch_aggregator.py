from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def aggregate_orders():
    print("Simulating order aggregation job...")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

with DAG(
    dag_id="ecommerce_batch_aggregator",
    default_args=default_args,
    description="Batch aggregation of e-commerce orders",
    schedule_interval="@daily",
    tags=["ecommerce", "batch"]
) as dag:

    aggregate_task = PythonOperator(
        task_id="aggregate_orders",
        python_callable=aggregate_orders
    )
