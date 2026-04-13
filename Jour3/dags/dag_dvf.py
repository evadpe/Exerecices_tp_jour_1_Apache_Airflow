from __future__ import annotations
import logging
import os
import tempfile
import io
from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL = "https://www.data.gouv.fr/fr/datasets/r/90a98de0-f562-4328-aa16-fe0dd1dca60f"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts = {}
        try:
            r_dvf = requests.head(DVF_URL, timeout=10)
            statuts["dvf_api"] = r_dvf.status_code < 400
        except Exception:
            statuts["dvf_api"] = False
        try:
            url_hdfs = f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}"
            r_hdfs = requests.get(url_hdfs, timeout=10)
            statuts["hdfs"] = r_hdfs.status_code == 200
        except Exception:
            statuts["hdfs"] = False
        if not statuts["dvf_api"] or not statuts["hdfs"]:
            from airflow.exceptions import AirflowException
            raise AirflowException("Source critique indisponible")
        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        annee = datetime.now().year
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")
        # Fichier local disponible (évite le téléchargement du fichier ~500 Mo)
        local_source = "/opt/airflow/dags/dvf_2023.csv"
        if os.path.exists(local_source):
            import shutil
            shutil.copy(local_source, local_path)
            logger.info(f"Fichier local utilisé : {local_source}")
        else:
            with requests.get(DVF_URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            logger.info(f"Fichier téléchargé depuis data.gouv.fr")
        logger.info(f"Taille : {os.path.getsize(local_path)} octets")
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        annee = datetime.now().year
        hdfs_file_path = f"{HDFS_RAW_PATH}/dvf_{annee}.csv"
        requests.put(f"{WEBHDFS_BASE_URL}{HDFS_RAW_PATH}?op=MKDIRS&user.name={WEBHDFS_USER}")
        url_create = f"{WEBHDFS_BASE_URL}{hdfs_file_path}?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
        r_init = requests.put(url_create, allow_redirects=False)
        if r_init.status_code == 307:
            with open(local_path, "rb") as f:
                requests.put(r_init.headers["Location"], data=f).raise_for_status()
        os.remove(local_path)
        return hdfs_file_path

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        url_open = f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}"
        response = requests.get(url_open, allow_redirects=True)
        df = pd.read_csv(io.BytesIO(response.content), low_memory=False, sep=',')
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        # Normaliser code_postal : pandas le lit parfois comme float (75008.0 → "75008")
        df['code_postal'] = df['code_postal'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)
        df = df[
            (df['type_local'] == 'Appartement') & 
            (df['code_postal'].astype(str).str.startswith('75')) &
            (df['surface_reelle_bati'].between(9, 500)) &
            (df['valeur_fonciere'] > 10000) &
            (df['nature_mutation'] == 'Vente')
        ].copy()
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        df['arrondissement'] = df['code_postal'].astype(str).str[-2:].astype(int)
        now = datetime.now()
        agregats = df.groupby('code_postal').agg(
            prix_m2_moyen=('prix_m2', 'mean'),
            prix_m2_median=('prix_m2', 'median'),
            prix_m2_min=('prix_m2', 'min'),
            prix_m2_max=('prix_m2', 'max'),
            nb_transactions=('prix_m2', 'count'),
            surface_moyenne=('surface_reelle_bati', 'mean'),
            arrondissement=('arrondissement', 'first')
        ).reset_index()
        agregats['annee'] = now.year
        agregats['mois'] = now.month
        stats_globales = {
            "annee": now.year, "mois": now.month,
            "nb_transactions_total": int(df.shape[0]),
            "prix_m2_median_paris": float(df['prix_m2'].median()),
            "prix_m2_moyen_paris": float(df['prix_m2'].mean()),
            "surface_mediane": float(df['surface_reelle_bati'].median())
        }
        return {"agregats": agregats.to_dict(orient='records'), "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        agregats = resultats.get("agregats", [])
        stats = resultats.get("stats_globales", {})
        upsert_query = """
            INSERT INTO prix_m2_arrondissement 
            (code_postal, arrondissement, annee, mois, prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max, nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen, prix_m2_median = EXCLUDED.prix_m2_median, nb_transactions = EXCLUDED.nb_transactions, updated_at = NOW();
        """
        for row in agregats:
            hook.run(upsert_query, parameters=(
                row['code_postal'], row['arrondissement'], row['annee'], row['mois'],
                float(row['prix_m2_moyen']), float(row['prix_m2_median']), float(row['prix_m2_min']),
                float(row['prix_m2_max']), int(row['nb_transactions']), float(row['surface_moyenne'])
            ))
        hook.run("""
            INSERT INTO stats_marche (annee, mois, nb_transactions_total, prix_m2_median_paris, prix_m2_moyen_paris, surface_mediane, date_calcul)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (annee, mois) DO UPDATE SET nb_transactions_total = EXCLUDED.nb_transactions_total;
        """, parameters=(stats['annee'], stats['mois'], stats['nb_transactions_total'], stats['prix_m2_median_paris'], stats['prix_m2_moyen_paris'], stats['surface_mediane']))
        return len(agregats)

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        now = datetime.now()
        records = hook.get_records("""
            SELECT arrondissement, prix_m2_median, prix_m2_moyen, nb_transactions FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s ORDER BY prix_m2_median DESC LIMIT 20;
        """, parameters=(now.year, now.month))
        rapport = "Arrondissement | Median (EUR/m2) | Moyen (EUR/m2) | Transactions\n" + "-"*65 + "\n"
        for r in records:
            rapport += f"{r[0]}e | {r[1]:,.0f} | {r[2]:,.0f} | {r[3]}\n"
        logger.info(rapport)
        return rapport

    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)

    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)

pipeline_dvf()