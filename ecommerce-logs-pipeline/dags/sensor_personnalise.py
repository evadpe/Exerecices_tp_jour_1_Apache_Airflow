from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="dag_sensor_fichier",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    attendre_fichier = FileSensor(
        task_id="attendre_go_txt",
        filepath="go.txt",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=600 
    )

    suite = PythonOperator(
        task_id="suite_du_workflow",
        python_callable=lambda: print("Fichier trouvé ! Je continue...")
    )

    attendre_fichier >> suite