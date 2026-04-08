Réponses aux questions du tp :

Q1 : Docker Executor Le docker-compose.yaml utilise LocalExecutor . Expliquez la différence entre LocalExecutor , CeleryExecutor et KubernetesExecutor . Dans quel contexte de production RTE devrait-il utiliser chacun ? Quelles sont les implications en termes de scalabilité et de ressources ?
- LoaclExecutor fait tout tout seul, il planifie et execeute les tâches, la scalabilité est nulle, en général on s'en sert pour tester le code.
- CeleryExecutor sépare le scheduler des workers, meilleur en terme de scalabilité pour distribuer les tâches
- KubernetesExecutor: pour chaque tâche, Airflow demande à Kubernetes de créer un mini-serveur (un Pod) qui n'existe que pour cette tâche et qui ferme après donc en terme de scalabilité c'est casi infini

Q2 : Volumes Docker et persistance des DAGs Le volume ./dags:/opt/airflow/dags permet de modifier les DAGs sans redémarrer le conteneur. Expliquez le mécanisme sous-jacent (bind mount vs volume nommé). Que se passerait-il si on supprimait ce mapping ? Quel serait l’impact en production sur un cluster Airflow multi-nœuds (plusieurs workers) ?
- le bindmount, ce que j'ai dans le tp, est un "pont" entre mon fichiers sur lon ordi et le fichier dans le contenur docker. Quand je fais la modif dans mon vscode, docker le voit et change automatiquement et airflow le voit et fait les modifs aussi. le volume nommé c'est docker qui gère tout et c'est plus compliqué d'aller le modifier. 
- Si on supprime ce mapping, airflow va démarrer mais je verrais pas mes dags dans mon interface et si docker ne verra pas mes modifs. 
- Le problème en prod c'est que si je suis sur 3 serveurs diiférents pour mes 3 workers, si je fais une modif sur mon ordi, il faudrait que je copie colle mes modfis sur les 3 serveurs 

Q3 — Idempotence et catchup Le DAG a catchup=False . Expliquez ce que ferait Airflow si
catchup=True et que le DAG est activé aujourd’hui avec un start_date au 1er janvier 2024. Qu’est-ce que l’idempotence d’un DAG et pourquoi est-ce critique pour un pipeline de données énergétiques ? Comment rendre les fonctions collecter_* idempotentes ?
- Si mon start_date est au 1er janvier 2024 et que j'active le bouton "ON" aujourd'hui avec catchup=True Airflow va se dire qu'il a du retard et va lancer des instances de mon DAG (au temps d'instances que de jours passés depuis le 01/01/2024) en même temps (perso sur mon ordi ça explose) mais le problème aussi est que le nombre de requêtes pas API n'est pas infini par jour donc ça va planter. L'Idempotence (ça je suis allée chercher sur internet je ne savais pas ce que c'était) c'est le fait que peu importe combien de fois je lance la même tâche avec les mêmes paramètres, le résultat final doit être exactement le même. Si mon dag n'est pas idempotent et que mon workflow plante à la fin, que je le modifie et le relance, il va relancer toute la collecte et ajouter les données en doubon/triplon au lieu d'écraser et remplacer par les nouvelles. Moi j'utilise date.today() donc si mon code plante ce soir et que je décide de le relancer demain, il récupèrera les données de demain et pas d'aujourd'hui donc c'est pas totalement idempotent en fonction du moment où je le lance. Je devrais utiliser la variable {{ ds }} d'Airflow (la date d'exécution prévue). Comme ça même si on est demain, le job d'hier ira chercher les données d'hier.

Q4 — Timezone et données temps-réel L’API éCO2mix retourne des données horodatées. Pourquoi le paramètre timezone=Europe/Paris est-il essentiel dans le contexte RTE ? Que peut-il se passer lors du passage à l’heure d’été (dernier dimanche de mars) si la timezone n’est pas correctement gérée dans le scheduler Airflow et dans les requêtes API ? Donnez un exemple concret de données corrompues ou manquantes.

- Airflow se base sur les horaires UTC (heure françasise en été) alors que Europe/Paris est l'horaire pour toute la France qui se met à jour à l'heure d'hiver donc si je le spécifie pas j'aurais des heures de décalage. Si la timezone est mal géré il pourrait louper une heure (par exemple 2h15 n'existe pas) ou se décaler pour tout le reste de l'année. Côté API, il pourrait avoir aussi des problèmes/mélanges dans les données si c'est mal géré. 

Captures d'écran à fournir : 

1. Interface Airflow UI avec le DAG energie_meteo_dag visible et en état success
![alt text](/captures_ecran/interface_ui_airflow_success.png)

2.  Vue Graph du DAG montrant les 5 tâches et leurs dépendances
![alt text](/captures_ecran/graph_DAG.png)

3.  Logs d’une exécution réussie de generer_rapport_energie avec le tableau affiché
![alt text](/captures_ecran/log_rapport_energie.png)

4. Onglet XCom de la tâche analyser_correlation montrant le dictionnaire d’alertes
![alt text](/captures_ecran/x_com_correlation.png)

5. Contenu du fichier JSON généré (via docker compose exec ... cat /tmp/rapport_energie_*.json)
![alt text](/captures_ecran/fichier_json_genere.png)
