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

with DAG("Update_Country_Dag", default_args=default_args, schedule_interval='0 18 * * *', catchup=False) as dag:
    # task1 = PythonOperator(task_id="correct_country_in_database", python_callable=update_tweets_loc)

    task1 = BashOperator(task_id="correct_country_in_database",
                         bash_command = 'python3 /Users/shivamraj/Documents/Project/utils/update_country.py')
                         # bash_command='python3 /opt/airflow/dags/update_country.py')

    # task1 >> task2
    task1

# Macros, Catchup
# Max active runs