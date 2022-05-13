from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG("Collect_Data_Dag", default_args=default_args, schedule_interval='0 18 * * *', catchup=False) as dag:

    data_src_5 = BashOperator(task_id="get_data_src_measures_taken_country_wise",
                              bash_command='python3 /opt/airflow/dags/scripts/dag_data_source_api_5.py')
    data_src_6 = BashOperator(task_id="get_data_src_donations",
                              bash_command='python3 /opt/airflow/dags/scripts/dag_data_source_api_6.py')
    data_src_7 = BashOperator(task_id="get_data_src_impacted_country_week_wise",
                              bash_command='python3 /opt/airflow/dags/scripts/dag_data_source_api_7.py')
    data_src_9 = BashOperator(task_id="get_data_src_age_category_weather_wise",
                              bash_command='python3 /opt/airflow/dags/scripts/dag_data_source_api_9.py')

    data_src_5 >> data_src_6 >> data_src_7 >> data_src_9
    # data_src_5

