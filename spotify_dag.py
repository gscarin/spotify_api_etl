from datetime import time, timedelta
import datetime
from airflow import DAG
from airflow.models.dag import ScheduleInterval
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

#importing spotify etl file
from spotify_etl import run_spotify_etl



default_args = {
    'owner' :  'airflow-gabriel',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021,8,31),
    'email': ['scarin3d@gmail.com'],
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}


dag = DAG(
    'spotify_dag',
    default_args = default_args,
    description = 'My first Airflow Dag',
    schedule_interval = timedelta(days = 1),
)


run_etl = PythonOperator(
    task_id = 'whole_etl',
    ##Function name to run etl
    python_callable = run_spotify_etl,
    dag = dag,
)

run_etl
