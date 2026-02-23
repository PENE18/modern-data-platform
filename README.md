# Plateforme Modern Data Lakehouse

Une plateforme de data lakehouse entièrement conteneurisée et prête pour la production, dédiée à l'analytique e-commerce. Construite sur Apache Iceberg avec une architecture médaillon (Bronze → Silver → Gold), orchestrée par Airflow, promue dans MinIO via Spark, interrogée via Dremio et Jupyter, et surveillée avec Prometheus et Grafana — le tout en local avec Docker Compose.

---

## Comment ça fonctionne

La plateforme s'exécute en quatre phases séquentielles :

```
PHASE 1 ── DAG Airflow
           Génère les CSV → Bronze (Parquet local) → Silver → Gold
                │
                ▼
PHASE 2 ── Job Spark + Iceberg
           Lit les Parquet locaux → écrit les tables Iceberg dans MinIO
           Le catalogue Nessie suit tous les snapshots et branches
                │
                ▼
PHASE 3 ── Interrogation & Exploration
           Dremio  → SQL sur les tables Iceberg via le catalogue Nessie
           Jupyter → PySpark · voyage dans le temps · évolution de schéma · analyse
                │
                ▼
PHASE 4 ── Supervision
           Prometheus → collecte des métriques de tous les services
           Grafana    → tableaux de bord · alertes · CPU / mémoire / tâches
```

---

## Stack Technologique

| Couche | Technologie | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.8 |
| Traitement | Apache Spark | 3.5 (1 master + 2 workers) |
| Format de table | Apache Iceberg | 1.5 |
| Catalogue | Project Nessie | 0.90.4 |
| Stockage objet | MinIO | latest |
| Moteur de requêtes | Dremio | latest |
| Notebooks | JupyterLab (PySpark) | spark-3.5.0 |
| Base de métadonnées | PostgreSQL | 15 |
| Supervision | Prometheus + Grafana | latest |

---

## Prérequis

- Docker Desktop avec **8 Go+ de RAM** alloués (Paramètres → Ressources)
- **20 Go+** d'espace disque libre
- Les ports suivants disponibles : `3000, 4040–4050, 5432, 7077, 8080, 8081, 8888, 9000, 9001, 9047, 9090, 19120`

---

## Démarrage

```bash
# Construire les images et démarrer tous les services
docker compose up --build -d

# Vérifier que tous les conteneurs tournent
docker compose ps
```

---

## URLs des Services

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| Dremio | http://localhost:9047 | assistant de première connexion |
| JupyterLab | http://localhost:8888 | token — voir ci-dessous |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Nessie API | http://localhost:19120 | — |
| PostgreSQL | localhost:5432 | admin / admin123 |

```bash
# Récupérer le token Jupyter
docker logs jupyter | grep "token="
```

---

## Phase 1 — Pipeline ETL Airflow

Le DAG `ecommerce_etl_pipeline` construit toute la pile médaillon sous forme de fichiers Parquet locaux.

### Exécution

1. Ouvrir http://localhost:8081 → se connecter (`admin` / `admin`)
2. Trouver le DAG **`ecommerce_etl_pipeline`**
3. Activer le switch **ON**
4. Cliquer sur **▶ Trigger DAG**
5. Observer l'exécution des 6 tâches en séquence

### Tâches du Pipeline

| # | Tâche | Description |
|---|---|---|
| 1 | `generate_sample_data` | Crée 1 000 clients, 200 produits, 5 000 commandes en CSV dans `data/raw/` |
| 2 | `ingest_to_bronze` | Charge les CSV dans `data/bronze/` en Parquet avec les colonnes `ingested_at` et `data_quality_flag` |
| 3 | `validate_data_quality` | Vérifications de nullité, de plages de valeurs et de validité des statuts sur la couche Bronze |
| 4 | `transform_to_silver` | Jointures commandes + clients + produits, suppression des annulations, dérivation des colonnes revenus et dates → `data/silver/` |
| 5 | `aggregate_to_gold` | Produit `daily_sales`, `customer_metrics`, `product_performance`, `regional_sales` → `data/gold/` |
| 6 | `generate_report` | Écrit les KPIs principaux dans `data/gold/kpi_report.json` |

### Fichiers Créés en Local

```bash
ls -lh data/bronze/
# customers.parquet   orders.parquet   products.parquet

ls -lh data/silver/
# orders_enriched.parquet

ls -lh data/gold/
# customer_metrics.parquet   daily_sales.parquet
# product_performance.parquet   regional_sales.parquet   kpi_report.json
```

Il est aussi possible de générer les données sans Airflow :

```bash
docker exec airflow-webserver python /opt/scripts/generate_data.py
```

---

## Phase 2 — Spark + Iceberg → MinIO + Nessie

Une fois les fichiers Parquet locaux créés, on les promeut en vraies tables Iceberg stockées dans MinIO et cataloguées par Nessie.

### Lancer le Job Iceberg

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/iceberg_operations.py
```

Ce job réalise dans l'ordre :

1. Création des namespaces Iceberg dans Nessie (`iceberg.bronze`, `iceberg.silver`, `iceberg.gold`)
2. Création des tables Bronze partitionnées :
   - `bronze.orders` — partitionné par `days(order_date)`
   - `bronze.products` — partitionné par `category`
   - `bronze.customers` — non partitionné
3. Chargement de tous les Parquet locaux dans ces tables Iceberg sur MinIO
4. Création de `silver.orders_enriched` — jointures, nettoyage, partitionné par `(order_year, order_month)`
5. Création de `gold.daily_sales` et `gold.product_performance`
6. Démonstration du voyage dans le temps, de l'évolution de schéma et des statistiques de partitions

### Cluster Spark

```
spark-master    :8080 (UI)   :7077 (RPC)   :4040–4050 (UIs des jobs)
spark-worker-1  2 Go · 2 cœurs
spark-worker-2  2 Go · 2 cœurs
```

L'image Docker personnalisée embarque tous les JARs nécessaires :

```
iceberg-spark-runtime-3.5_2.12-1.5.2.jar
nessie-spark-extensions-3.5_2.12-0.90.4.jar
hadoop-aws-3.3.4.jar
aws-java-sdk-bundle-1.12.262.jar
delta-spark_2.12-3.1.0.jar
postgresql-42.7.1.jar
```

Packages Python : `pyspark==3.5.3`, `pyarrow==14`, `pyiceberg==0.6`, `pandas>=2.0`, `boto3`, `faker`

---

## Phase 3 — Interrogation avec Dremio & Jupyter

### Dremio — SQL sur Iceberg

Dremio se connecte à la fois à MinIO (fichiers bruts) et à Nessie (catalogue Iceberg). Configuration unique sur http://localhost:9047.

**Ajouter MinIO comme source S3 compatible :**

```
Type :             Amazon S3
Nom :              minio
Access Key :       minioadmin
Secret Key :       minioadmin123
                   ✅ Activer le mode de compatibilité
                   ✅ Désactiver SSL (Options avancées → Chiffrement)

Propriétés de connexion :
  fs.s3a.endpoint               = minio:9000
  fs.s3a.path.style.access      = true
  fs.s3a.connection.ssl.enabled = false
```

**Ajouter Nessie comme source de catalogue :**

```
Type :             Nessie
Nom :              nessie
URL du point d'entrée : http://nessie:19120/api/v2
Authentification : Aucune
AWS Root Path :    warehouse
Access Key :       minioadmin
Secret Key :       minioadmin123

Propriétés de connexion (identiques à MinIO ci-dessus)
```

**Vérification :**

```sql
SHOW SCHEMAS IN nessie;
-- nessie.bronze   nessie.silver   nessie.gold

SELECT * FROM nessie.bronze.orders LIMIT 10;
```

**Exemples de requêtes métier :**

```sql
-- Tendance de revenus quotidienne
SELECT date, num_orders, total_revenue, avg_order_value
FROM nessie.gold.daily_sales
ORDER BY date DESC LIMIT 14;

-- Top 10 clients par valeur vie client
SELECT customer_id, total_orders, lifetime_value
FROM nessie.gold.customer_metrics
ORDER BY lifetime_value DESC LIMIT 10;

-- Revenus par catégorie de produits
SELECT category, SUM(total_revenue) AS revenu, SUM(units_sold) AS unites
FROM nessie.gold.product_performance
GROUP BY category ORDER BY revenu DESC;

-- Voyage dans le temps Iceberg
SELECT COUNT(*) FROM nessie.bronze.orders
VERSION AS OF <snapshot_id>;
```

Dremio expose également **Arrow Flight** (`:45678`) et **ODBC/JDBC** (`:31010`) pour les outils de BI.

---

### JupyterLab — Exploration PySpark & Iceberg

```bash
# Récupérer le token d'accès
docker logs jupyter | grep "token="
```

Ouvrir http://localhost:8888 puis créer un nouveau notebook.

**Connexion à Spark et lecture Iceberg :**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exploration") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.format("iceberg").load("iceberg.gold.daily_sales")
df.orderBy("order_date", ascending=False).show(10)
```

**Voyage dans le temps :**

```python
spark.sql("""
    SELECT COUNT(*) AS enregistrements
    FROM iceberg.bronze.orders
    VERSION AS OF 1234567890
""").show()
```

**Évolution de schéma — sans interruption de service :**

```python
spark.sql("""
    ALTER TABLE iceberg.bronze.orders
    ADD COLUMN remise_percent DOUBLE
""")

# Les anciennes requêtes continuent de fonctionner à l'identique
spark.sql("SELECT order_id, total_amount FROM iceberg.bronze.orders LIMIT 5").show()
```

**Test de performance — élagage de partitions :**

```python
# Scan complet
spark.sql("SELECT COUNT(*) FROM iceberg.bronze.orders").show()

# Avec filtre de partition — ne lit que les fichiers concernés (~100x plus rapide)
spark.sql("""
    SELECT COUNT(*) FROM iceberg.bronze.orders
    WHERE order_date = '2024-02-15'
""").show()
```

**Lecture directe des Parquet Gold avec pandas :**

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('/home/jovyan/data/gold/daily_sales.parquet')
df.sort_values('date').plot(x='date', y='total_revenue', figsize=(14, 5), title='Revenus Quotidiens')
plt.tight_layout()
plt.show()
```

---

## Phase 4 — Supervision

### Prometheus — Collecte des Métriques

Ouvrir http://localhost:9090. Vérifier les cibles de collecte sur `/targets` — toutes doivent être vertes.

Requêtes PromQL utiles :

```promql
# Utilisation CPU
rate(process_cpu_seconds_total[5m])

# Mémoire (Mo)
process_resident_memory_bytes / 1024 / 1024

# Workers Spark actifs
spark_master_aliveWorkers

# Durées des tâches Airflow
airflow_task_duration

# Heap JVM (Dremio)
jvm_memory_bytes_used{area="heap"}
```

> Pour activer les métriques Spark : ajouter `spark.ui.prometheus.enabled=true` dans `spark/spark-defaults.conf` puis reconstruire avec `docker compose build spark-master`.

### Grafana — Tableaux de Bord & Alertes

Ouvrir http://localhost:3000 (`admin` / `admin`).

**Ajouter Prometheus comme source de données :**
Connexions → Sources de données → Prometheus → URL : `http://prometheus:9090` → Sauvegarder & Tester

**Panneaux recommandés pour le tableau de bord :**

| Panneau | Requête | Visualisation |
|---|---|---|
| Utilisation CPU | `rate(process_cpu_seconds_total[5m])` | Série temporelle |
| Mémoire (Mo) | `process_resident_memory_bytes / 1024 / 1024` | Jauge |
| Workers Spark | `spark_master_aliveWorkers` | Stat |
| Tâches Airflow | `airflow_task_instance_created_count` | Stat |

**Importer un tableau de bord JVM prêt à l'emploi :** Tableaux de bord → Importer → ID `4701` → sélectionner Prometheus → Importer

**Alerte mémoire :**
```
Condition :  process_resident_memory_bytes > 4000000000
Durée :      5 minutes
Sévérité :   Avertissement
```

### Vérification Rapide de Santé

```bash
echo "=== Vérification de la plateforme ===" && \
echo -n "Spark :      " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080 && \
echo -n "Airflow :    " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8081/health && \
echo -n "Dremio :     " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:9047 && \
echo -n "MinIO :      " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:9001 && \
echo -n "Prometheus : " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:9090 && \
echo -n "Grafana :    " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000 && \
echo -n "Nessie :     " && curl -s -o /dev/null -w "%{http_code}\n" http://localhost:19120/api/v2/config && \
echo "=== Terminé ==="
```

Tous les services doivent retourner `200`.

---

## Modèle de Données

```
Clients (1 000 enregistrements)
├── customer_id (PK), email, first_name, last_name
├── city, state, registration_date
└── customer_segment  (Premium | Standard | Basic)

Produits (200 enregistrements)
├── product_id (PK), product_name
├── category  (Electronics | Clothing | Home | Sports | Books)
└── price, stock_quantity

Commandes (5 000 enregistrements)
├── order_id (PK), customer_id (FK), product_id (FK)
├── order_date, quantity, total_amount
├── status  (completed | pending | cancelled)
└── payment_method  (Credit Card | PayPal | Debit Card)
```

**Tables de la couche Gold :**

| Table | Colonnes principales |
|---|---|
| `daily_sales` | date · num_orders · total_revenue · unique_customers · avg_order_value |
| `customer_metrics` | customer_id · total_orders · lifetime_value · days_active |
| `product_performance` | product_id · category · units_sold · total_revenue |
| `regional_sales` | state · num_orders · total_revenue · unique_customers |
| `kpi_report.json` | total_revenue · total_orders · avg_order_value · total_customers |

---

## Structure du Projet

```
modern-data-platform/
├── docker-compose.yml
├── start.sh
│
├── airflow/
│   └── dags/
│       └── ecommerce_etl_pipeline.py   ← DAG ETL en 6 tâches
│
├── spark/
│   ├── Dockerfile                       ← Image personnalisée avec JARs Iceberg/Nessie
│   ├── spark-defaults.conf
│   └── jobs/
│       └── iceberg_operations.py        ← Promotion Iceberg, voyage dans le temps, évolution de schéma
│
├── scripts/
│   └── generate_data.py                 ← Générateur de données autonome
│
├── sql/
│   └── init.sql                         ← Initialisation PostgreSQL
│
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│
└── data/                                ← Créé à l'exécution par Airflow
    ├── raw/                             ← Fichiers CSV sources
    ├── bronze/                          ← Parquet brut (ingéré localement)
    ├── silver/                          ← Parquet nettoyé et enrichi
    └── gold/                            ← Parquet agrégé + kpi_report.json
```

---

## Commandes Utiles

```bash
# Démarrer la plateforme
docker compose up --build -d

# Vérifier le statut
docker compose ps

# Suivre les logs d'un service
docker compose logs -f airflow-webserver
docker compose logs -f spark-master

# Redémarrer un service
docker compose restart dremio

# Lancer le job Spark Iceberg manuellement
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/iceberg_operations.py

# Générer les données de façon autonome
docker exec airflow-webserver python /opt/scripts/generate_data.py

# Ouvrir un shell PostgreSQL
docker exec -it postgres psql -U admin -d airflow

# Arrêter (conserver les volumes)
docker compose down

# Arrêter et tout effacer
docker compose down -v
```

---

## Résolution de Problèmes

**Les services ne démarrent pas** — Docker a besoin d'au moins 8 Go de RAM. Docker Desktop → Paramètres → Ressources → augmenter la mémoire.

**Conflits de ports** — modifier les ports côté hôte dans `docker-compose.yml`.

**Échec du pipeline Airflow** — consulter les logs des tâches dans l'interface Airflow. S'assurer que `data/raw/` contient les fichiers CSV avant l'exécution de la tâche d'ingestion.

**Le job Spark ne peut pas atteindre MinIO** — vérifier que les buckets `warehouse` et `lakehouse` existent dans la console MinIO (http://localhost:9001) et que `minio` affiche `healthy` dans `docker compose ps`.

**Dremio ne voit aucune table Iceberg** — le job Spark (Phase 2) doit se terminer avant que Dremio puisse lire quoi que ce soit. Exécuter le `spark-submit` puis actualiser la source Nessie dans l'interface Dremio.

**Grafana affiche "Aucune donnée"** — régler la plage temporelle sur "Dernière heure". Si des cibles Prometheus sont en erreur, inspecter http://localhost:9090/targets.

**Jupyter ne peut pas se connecter à Spark** — vérifier que `spark-master` est en bonne santé et que `spark-defaults.conf` est correctement monté dans le conteneur Jupyter.

---

## Passage en Production

| Composant | Configuration locale | Option production |
|---|---|---|
| Stockage objet | MinIO | AWS S3 / Google GCS |
| Spark | Cluster Docker 3 nœuds | EMR / Dataproc (autoscaling) |
| Moteur de requêtes | Dremio OSS | Dremio Cloud |
| Orchestration | Airflow LocalExecutor | MWAA / Airflow CeleryExecutor |
| Catalogue | Nessie OSS | Nessie Cloud / AWS Glue |
| Streaming | — | Apache Kafka + Spark Structured Streaming |
| Transformations | — | dbt sur les couches Silver/Gold |

---

## Ressources

- [Documentation Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [Project Nessie](https://projectnessie.org/)
- [Dremio University](https://www.dremio.com/university/)
- [Designing Data-Intensive Applications](https://dataintensive.net/)
- [r/dataengineering](https://reddit.com/r/dataengineering)




