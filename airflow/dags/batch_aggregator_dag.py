from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_batch_aggregator',
    default_args=default_args,
    description='Runs batch aggregator script daily',
    schedule_interval='@daily',
    catchup=False,
)

run_batch_aggregator = BashOperator(
    task_id='run_batch_aggregator_script',
    bash_command='python /opt/airflow/dags/../../../batch/batch_aggregator.py',
    dag=dag,
)
