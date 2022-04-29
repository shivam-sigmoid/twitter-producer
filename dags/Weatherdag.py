from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
with DAG("Weather_Dag", default_args=default_args, schedule_interval='0 18 * * *',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:


    task1 >> task2 >> task3

# Macros, Catchup
# Max active runs
