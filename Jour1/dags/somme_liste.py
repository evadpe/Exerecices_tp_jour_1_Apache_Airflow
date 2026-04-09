from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id="dag_xcom_somme",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def xcom_dag():

    @task()
    def generer_liste():
        return [10, 20, 30, 40, 50]

    @task()
    def calculer_somme(nombres):
        resultat = sum(nombres)
        return resultat

    @task()
    def afficher_resultat(total):
        print(f"Le résultat final est : {total}")
    ma_liste = generer_liste()
    le_total = calculer_somme(ma_liste)
    afficher_resultat(le_total)

xcom_dag_inst = xcom_dag()