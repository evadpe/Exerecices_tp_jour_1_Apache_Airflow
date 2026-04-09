from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False, 
}


def get_date():
    print(f"La date du jour est : {datetime.now()}")

with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    description='TP Jour 1 : Premier DAG avec 3 tâches',
    schedule_interval=None,
    start_date=datetime(2026, 4, 8),
    catchup=False,
    tags=['tp', 'jour1'],  
) as dag:

    t1 = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Début du workflow"'
    )
    t2 = PythonOperator(
        task_id='retourne_date',
        python_callable=get_date
    )
    t3 = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"'
    )


    t1 >> t2 >> t3