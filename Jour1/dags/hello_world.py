from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")

# On définit le DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='Mon premier DAG de Master',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Tâche 1 : Python
    task1 = PythonOperator(
        task_id='dire_bonjour',
        python_callable=print_hello
    )

    # Tâche 2 : Bash
    task2 = BashOperator(
        task_id='afficher_date',
        bash_command='date'
    )
    task1 >> task2