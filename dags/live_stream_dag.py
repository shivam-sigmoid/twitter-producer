from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG("live_stream_Dag", default_args=default_args, schedule_interval='0 18 * * *', catchup=False) as dag:
    task1 = BashOperator(task_id="producer_twitter_data",
                         bash_command='python3 /opt/airflow/dags/scripts/producer_tweets.py')

    task2 = BashOperator(task_id="consumer_twitter_data",
                         bash_command='python3 /opt/airflow/dags/scripts/consumer_tweets.py')

    # task2
    [task1, task2]

