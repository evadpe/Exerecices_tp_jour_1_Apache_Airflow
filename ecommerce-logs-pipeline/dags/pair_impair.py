from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import random

def choisir_chemin():
    nombre = random.randint(1, 100)
    print(f"Nombre généré : {nombre}")
    if nombre % 2 == 0:
        return "tache_pair"
    else:
        return "tache_impair"

with DAG(
    dag_id="dag_branchement_pair_impair",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    choix = BranchPythonOperator(
        task_id="choisir_chemin",
        python_callable=choisir_chemin
    )

    t_pair = EmptyOperator(task_id="tache_pair")
    t_impair = EmptyOperator(task_id="tache_impair")
    t_finale = EmptyOperator(
        task_id="tache_finale", 
        trigger_rule="none_failed_min_one_success"
    )

    choix >> [t_pair, t_impair] >> t_finale