import pendulum
import random
import logging
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)
default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}
@dag(
    dag_id="tp_ecommerce_full_hdfs",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["tp_jour2", "hdfs", "branching"]
)
def ecommerce_full_pipeline():
    @task()
    def generer_logs_journaliers():
        return [f"log_{i}.txt" for i in range(random.randint(2, 5))]
    @task()
    def uploader_vers_hdfs(fichiers):
        print(f"Upload de {len(fichiers)} fichiers vers HDFS...")
        return "/user/airflow/raw/ecommerce/logs_du_jour.txt"
    attendre_donnees_hdfs = FileSensor(
        task_id="hdfs_sensor_check",
        filepath="/tmp/hdfs_simulation.txt",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=300
    )
    @task()
    def analyser_logs_hdfs(chemin_hdfs):
        taux_erreur = random.uniform(0, 10)
        print(f"Analyse du fichier {chemin_hdfs} : Taux d'erreur = {taux_erreur:.2f}%")
        return taux_erreur
    def verifier_alerte_critique(**context):
        taux = context['ti'].xcom_pull(task_id='analyser_logs_hdfs')
        if taux > 5.0:
            return "alerte_equipe_ops"
        else:
            return "archiver_logs_hdfs"

    decision_alerte = BranchPythonOperator(
        task_id="decision_alerte",
        python_callable=verifier_alerte_critique
    )
    alerte_ops = EmptyOperator(task_id="alerte_equipe_ops")
    @task()
    def archiver_logs_hdfs():
        return True
    fichiers = generer_logs_journaliers()
    chemin = uploader_vers_hdfs(fichiers)
    chemin >> attendre_donnees_hdfs >> (taux := analyser_logs_hdfs(chemin))
    taux >> decision_alerte
    decision_alerte >> [alerte_ops, archiver_logs_hdfs()]

dag_final = ecommerce_full_pipeline()