from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('s3_to_rds_dag',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    etl_to_rds = BashOperator(
        task_id='etl_to_rds',
        bash_command='python3 /opt/airflow/batch/s3_to_rds_etl.py'
    )

    etl_to_rds
