from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('kafka_to_s3_dag',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    consume_kafka_data = BashOperator(
        task_id='consume_kafka_data',
        bash_command='spark-submit /opt/airflow/batch/spark_streaming_job.py'
    )

    consume_kafka_data
