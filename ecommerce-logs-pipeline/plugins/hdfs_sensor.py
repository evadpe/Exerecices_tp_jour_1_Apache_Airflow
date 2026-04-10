import requests
from airflow.sensors.base import BaseSensorOperator


class HdfsFileSensor(BaseSensorOperator):
    template_fields = ("hdfs_path",)

    def __init__(self, hdfs_path: str, namenode_url: str, hdfs_user: str = "root", **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.namenode_url = namenode_url.rstrip("/")
        self.hdfs_user = hdfs_user

    def poke(self, context) -> bool:
        url = f"{self.namenode_url}/webhdfs/v1{self.hdfs_path}"
        params = {"op": "GETFILESTATUS", "user.name": self.hdfs_user}

        self.log.info("Vérification WebHDFS : GET %s", url)
        try:
            resp = requests.get(url, params=params, timeout=5)

            if resp.status_code == 200:
                size = resp.json()["FileStatus"]["length"]
                self.log.info(
                    "Fichier trouvé : %s (%d bytes)", self.hdfs_path, size
                )
                return True

            if resp.status_code == 404:
                self.log.info("Fichier absent pour l'instant : %s", self.hdfs_path)
                return False
            self.log.warning(
                "Réponse inattendue du NameNode : HTTP %d — %s",
                resp.status_code,
                resp.text[:200],
            )
            return False

        except requests.exceptions.ConnectionError as exc:
            self.log.warning("Impossible de joindre le NameNode : %s", exc)
            return False
        except Exception as exc:
            self.log.warning("Erreur inattendue lors du poke : %s", exc)
            return False
