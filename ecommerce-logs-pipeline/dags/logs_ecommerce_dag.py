import subprocess
import logging
import os
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, PythonOperator

log = logging.getLogger(__name__)

SEUIL_ERREUR_PCT = 5.0

default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="tp_ecommerce_full_hdfs",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["tp_jour2", "hdfs", "branching"],
)
def ecommerce_full_pipeline():

    @task()
    def generer_logs_journaliers(**context):
        execution_date = context["ds"]
        fichier_sortie = f"/tmp/access_{execution_date}.log"
        script_path = "/opt/airflow/scripts/generer_logs.py"

        subprocess.run(
            ["python3", script_path, execution_date, "1000", fichier_sortie],
            check=True,
        )

        taille = os.path.getsize(fichier_sortie)
        log.info("Fichier généré : %s (%d octets)", fichier_sortie, taille)
        return fichier_sortie

    @task()
    def uploader_vers_hdfs(fichier_local, **context):
        execution_date = context["ds"]
        chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
        transfer = f"/tmp/access_{execution_date}_transfer.log"

        scheduler_id = subprocess.check_output(
            ["docker", "ps", "--filter", "name=scheduler", "--format", "{{.ID}}"]
        ).decode().strip().splitlines()[0]

        subprocess.run(["docker", "cp", f"{scheduler_id}:{fichier_local}", transfer], check=True)
        subprocess.run(["docker", "cp", transfer, f"namenode:/tmp/access_{execution_date}.log"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/ecommerce/logs/raw"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/tmp/access_{execution_date}.log", chemin_hdfs], check=True)

        log.info("Fichier uploadé vers HDFS : %s", chemin_hdfs)
        return chemin_hdfs

    @task()
    def hdfs_file_sensor(chemin_hdfs, **context):
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-test", "-e", chemin_hdfs]
        )
        if result.returncode != 0:
            raise FileNotFoundError(f"Fichier absent dans HDFS : {chemin_hdfs}")
        log.info("Fichier confirmé dans HDFS : %s", chemin_hdfs)

    @task()
    def analyser_logs_hdfs(chemin_hdfs, **context):
        import re
        from collections import Counter

        execution_date = context["ds"]
        fichier_local = f"/tmp/logs_analyse_{execution_date}.txt"
        fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

        contenu = subprocess.check_output(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", chemin_hdfs]
        ).decode()

        with open(fichier_local, "w") as f:
            f.write(contenu)

        lignes = contenu.splitlines()
        total = len(lignes)
        erreurs = sum(1 for l in lignes if any(f' {code} ' in l for code in
                      ['400', '401', '403', '404', '500', '503']))

        taux_pct = (erreurs / total * 100) if total > 0 else 0
        log.info("Total: %d, Erreurs: %d, Taux: %.2f%%", total, erreurs, taux_pct)

        # Résumé des status codes
        status_counts = Counter()
        url_counts = Counter()
        for ligne in lignes:
            m = re.search(r'"(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS) (\S+) HTTP/\S+" (\d{3})', ligne)
            if m:
                url_counts[m.group(1)] += 1
                status_counts[m.group(2)] += 1

        log.info("=== Résumé des status codes ===")
        for code, count in sorted(status_counts.items()):
            log.info("  %s : %d", code, count)

        log.info("=== Top 5 URLs ===")
        for url, count in url_counts.most_common(5):
            log.info("  %s : %d requêtes", url, count)

        with open(fichier_taux, "w") as f:
            f.write(f"{erreurs} {total}")

        return taux_pct

    def brancher_selon_taux_erreur(**context):
        execution_date = context["ds"]
        fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

        with open(fichier_taux, "r") as f:
            erreurs, total = map(int, f.read().strip().split())

        taux_pct = (erreurs / total * 100) if total > 0 else 0
        log.info("Taux d'erreur : %.2f%% (%d/%d)", taux_pct, erreurs, total)

        if taux_pct > SEUIL_ERREUR_PCT:
            log.warning("[ALERTE] Taux critique : %.2f%%", taux_pct)
            return "alerter_equipe_ops"
        else:
            log.info("[OK] Taux dans les seuils : %.2f%%", taux_pct)
            return "archiver_rapport_ok"

    t_branch = BranchPythonOperator(
        task_id="decision_alerte",
        python_callable=brancher_selon_taux_erreur,
    )

    def alerter_equipe_ops(**context):
        log.warning(
            "[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du %s. "
            "Vérifiez les serveurs web.",
            context["ds"],
        )

    def archiver_rapport_ok(**context):
        log.info(
            "[OK] Taux d'erreur dans les seuils normaux pour les logs du %s.",
            context["ds"],
        )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )
    @task(trigger_rule="none_failed_min_one_success")
    def archiver_logs_hdfs(**context):
        execution_date = context["ds"]
        source = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
        destination = f"/data/ecommerce/logs/processed/access_{execution_date}.log"

        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/ecommerce/logs/processed"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mv", source, destination], check=True)

        log.info("Fichier archivé dans la zone processed : %s", destination)
    fichier = generer_logs_journaliers()
    chemin = uploader_vers_hdfs(fichier)
    hdfs_file_sensor(chemin) >> analyser_logs_hdfs(chemin) >> t_branch >> [t_alerte, t_archive_ok] >> archiver_logs_hdfs()


dag_final = ecommerce_full_pipeline()
