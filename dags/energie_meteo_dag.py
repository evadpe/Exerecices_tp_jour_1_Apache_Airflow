from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import pendulum
import requests
import logging
import json

REGIONS = {
    "Ile-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine": {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France": {"lat": 50.6292, "lon": 3.0573},
}

default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def verifier_apis():
    logging.info("Vérification des APIs...")
    return True

def collecter_meteo_regions():
    resultats = {}
    for region, coords in REGIONS.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&daily=sunshine_duration,wind_speed_10m_max&timezone=Europe/Paris&forecast_days=1"
        resp = requests.get(url)
        data = resp.json()
        resultats[region] = {
            "ensoleillement_h": data["daily"]["sunshine_duration"][0] / 3600,
            "vent_kmh": data["daily"]["wind_speed_10m_max"][0]
        }
    return resultats

def collecter_production_electrique():
    # Dataset RTE eCO2mix régional
    url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records?limit=50"
    resp = requests.get(url)
    return resp.json().get("results", [])

def analyser_correlation(**context):
    ti = context['ti']
    meteo = ti.xcom_pull(task_ids='collecter_meteo_regions')
    prod = ti.xcom_pull(task_ids='collecter_production_electrique')
    
    analyse_complete = {}
    
    for region, data_meteo in meteo.items():
        # Recherche de la région dans les données de prod
        stats_prod = next((item for item in prod if item.get('libelle_region') == region), {})
        
        # On convertit les valeurs en float pour éviter le crash (et on gère le None/Vide)
        try:
            solaire = float(stats_prod.get("solaire", 0) or 0)
            eolien = float(stats_prod.get("eolien", 0) or 0)
        except (ValueError, TypeError):
            solaire = 0.0
            eolien = 0.0
        
        # Maintenant la comparaison fonctionne !
        statut = "OK" if (solaire > 0 or eolien > 0) else "ALERTE"
        
        analyse_complete[region] = {
            "ensoleillement_h": data_meteo["ensoleillement_h"],
            "vent_kmh": data_meteo["vent_kmh"],
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": statut
        }
        
    logging.info(f"Analyse terminée pour {len(analyse_complete)} régions")
    return analyse_complete

def generer_rapport_energie(**context):
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation")
    
    # Si analyser_correlation a échoué ou n'a rien renvoyé
    if not analyse or not isinstance(analyse, dict):
        logging.error("Données d'analyse corrompues ou absentes")
        return None

    today = date.today().isoformat()
    
    print("\n" + "=" * 95)
    print(f"  RAPPORT ENERGIE & METEO — RTE — {today}")
    print("=" * 95)
    print(
        f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} "
        f"{'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>10}"
    )
    print("-" * 95)
    
    for region, data in analyse.items():
        print(
            f"{region:<25} "
            f"{data['ensoleillement_h']:>10.1f} "
            f"{data['vent_kmh']:>12.1f} "
            f"{data['solaire_mw']:>13.0f} "
            f"{data['eolien_mw']:>12.0f} "
            f"{data['statut']:>10}"
        )
    print("=" * 95 + "\n")
    
    rapport = {
        "date": today,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r["statut"] == "ALERTE"),
            "regions_en_alerte": [
                r for r, d in analyse.items() if d["statut"] == "ALERTE"
            ],
        },
    }
    
    chemin = f"/tmp/rapport_energie_{today}.json"
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)
        
    logging.info(f"Rapport sauvegardé : {chemin}")
    return chemin

with DAG(
    dag_id="energie_meteo_dag",
    default_args=default_args,
    schedule="0 6 * * *", 
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["rte", "tp_jour1"]
) as dag:

    t1 = PythonOperator(task_id="verifier_apis", python_callable=verifier_apis)
    t2 = PythonOperator(task_id="collecter_meteo_regions", python_callable=collecter_meteo_regions)
    t3 = PythonOperator(task_id="collecter_production_electrique", python_callable=collecter_production_electrique)
    t4 = PythonOperator(task_id="analyser_correlation", python_callable=analyser_correlation)
    t5 = PythonOperator(task_id="generer_rapport_energie", python_callable=generer_rapport_energie)

    t1 >> [t2, t3] >> t4 >> t5