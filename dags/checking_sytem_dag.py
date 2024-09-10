from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Instantiate the DAG object
with DAG(
    'checking_sytem_dag',
    default_args=default_args,
    description='A DAG that lists files in /opt/airflow/dags',
    schedule_interval=None,
    catchup=False
) as dag:

    # Define the tasks
    hello_task = BashOperator(
        task_id='hello_airflow_task',
        bash_command='ls /opt/airflow/dags'
    )

    # Set the task dependencies
    hello_task
