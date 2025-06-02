from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def convert_to_parquet():
    print("Simulating conversion to Parquet format...")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

with DAG(
    dag_id="parquet_processing_dag",
    default_args=default_args,
    description="Parquet conversion of raw e-commerce data",
    schedule_interval="@daily",
    tags=["ecommerce", "parquet"]
) as dag:

    parquet_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet
    )
