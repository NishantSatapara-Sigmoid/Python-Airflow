from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from task_1 import write_csv


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 16),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
     "retry_delay": timedelta(seconds=5),
}

dag = DAG("Assignment",default_args=default_args,schedule_interval='@daily',catchup=False,template_searchpath=['/usr/local/airflow/sql_files'])

t1 = PythonOperator(task_id='api_call', python_callable=write_csv, retry_delay=timedelta(seconds=15),dag=dag)

t2 = BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/Weather_Data.csv',
                  retries=2, retry_delay=timedelta(seconds=15),dag=dag)

t3 = PostgresOperator(task_id='create_table', postgres_conn_id="postgres_assignment", sql="create_table.sql",dag=dag)

t4 = PostgresOperator(task_id='insert_into_table', postgres_conn_id="postgres_assignment", sql="insert_into_table.sql",dag=dag)

t1 >> t2 >> t3 >> t4