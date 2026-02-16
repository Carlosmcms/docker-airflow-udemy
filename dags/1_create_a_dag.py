from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2026, 2, 14),
  "email": ["airflow@airflow.com"],
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 1,
  "retry_delay": timedelta(minutes=5),
  'end_date': datetime(2026, 2, 16),
}

dag = DAG("first_dag", default_args=default_args, schedule_interval=timedelta(1))

task1 = BashOperator(
  task_id = "print_date",
  bash_command = "date",
  retries = 2,
  dag = dag
)

bash_command = """
  echo "This is a test task"
"""

task2 = BashOperator(
  task_id = "print_first_task",
  bash_command = bash_command,
  retries = 2,
  dag = dag
)

task1.set_downstream(task2)
