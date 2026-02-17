from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator

import sys
sys.path.append('../helpers')
from datacleaner import data_cleaner

default_args = {
  'owner': 'Airflow',
  'start_date': datetime(2026, 2, 16),
  'retries': 1,
  'retry_delay': timedelta(seconds=5)
}

dag = DAG(
  'store_dag',
  default_args = default_args,
  schedule_interval = '@daily',
  template_searchpath = ['/usr/local/airflow/sql_files'],
  catchup = False
)

# Task 1: Check of the source file exists in the input directory location
check_file_exists = BashOperator(
  task_id = 'check_file_exists',
  bash_command = 'shasum ~/store_files_airflow/raw_store_transactions.csv',
  retries = 2,
  retry_delay = timedelta(seconds = 15),
  dag = dag
)

# Task 2: Remove unwanted characters from store_location column in .csv
clean_raw_csv = PythonOperator (
  task_id = 'clean_raw_csv',
  python_callable = data_cleaner,
  dag = dag
)

# Task 3: Create table clean_store_transactions in MySQL database
create_table_clean_store_transactions = MySqlOperator(
  task_id = 'create_mysql_table',
  mysql_conn_id = "mysql_conn",
  sql = "create_table.sql",
  dag = dag
)

check_file_exists >> clean_raw_csv >> create_table_clean_store_transactions