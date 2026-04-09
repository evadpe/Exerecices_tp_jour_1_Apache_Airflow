import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
import requests

@dag(
    dag_id="energie_meteo_dynamic_mapping", # Un ID différent pour pas de conflit
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["rte", "exercice_2"]
)
def pipeline_dynamique():
    @task()
    def charger_config():
        return Variable.get("regions_energie", deserialize_json=True)
    @task()
    def extraire_meteo_region(region_config: dict):
        print(f"Extraction pour {region_config['nom']}")
        return {"region": region_config['nom'], "status": "Done"}
    @task()
    def analyser_resultats(resultats_liste: list):
        print(f"J'ai reçu {len(resultats_liste)} rapports de régions.")
    configs = charger_config()
    rapports = extraire_meteo_region.expand(region_config=configs)
    analyser_resultats(rapports)
dag_final = pipeline_dynamique()