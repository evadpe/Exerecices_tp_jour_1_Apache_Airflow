"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
"""
import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# En interne Docker, on utilise le nom du service défini dans docker-compose
WEB_HDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEB_HDFS_USER = "root"

class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEB_HDFS_BASE_URL, user: str = WEB_HDFS_USER):
        self.base_url = base_url.rstrip('/')
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        path = path.lstrip('/')
        url = f"{self.base_url}/{path}?op={op}&user.name={self.user}"
        for key, value in params.items():
            url += f"&{key}={value}"
        return url

    def mkdirs(self, hdfs_path: str) -> bool:
        """Crée un répertoire (et ses parents) dans HDFS."""
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url)
        if response.status_code == 200:
            return response.json().get('boolean', False)
        response.raise_for_status()

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS en 2 étapes.
        """
        # Étape 1 : Demande au NameNode
        url = self._url(hdfs_path, "CREATE", overwrite="true")
        # On interdit la redirection automatique pour récupérer l'URL du DataNode
        response = requests.put(url, allow_redirects=False)
        
        if response.status_code == 307:
            datanode_url = response.headers['Location']
            # Étape 2 : Envoi réel des données au DataNode
            with open(local_file_path, 'rb') as f:
                upload_response = requests.put(datanode_url, data=f)
                upload_response.raise_for_status()
                return hdfs_path
        else:
            response.raise_for_status()

    def open(self, hdfs_path: str) -> bytes:
        """Lit le contenu d'un fichier HDFS."""
        url = self._url(hdfs_path, "OPEN")
        # Ici on laisse requests gérer la redirection (allow_redirects=True par défaut)
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier ou répertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url)
        return response.status_code == 200

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un répertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get('FileStatuses', {}).get('FileStatus', [])