# framework-elt-python

Framework Python générique d'**ELT** (Extract → Load → Transform) avec supervision et
traçabilité des traitements via **Power BI**.

Projet de Fin d'Année — ENSEM (Université Hassan II de Casablanca), filière Génie Logiciel
et Digitalisation, réalisé au sein du département IT de Marjanemall (2025–2026).

L'objectif est de proposer une alternative légère et open source à l'orchestration
Azure Data Factory : le pipeline est entièrement piloté par un fichier JSON, et chaque
exécution est tracée dans un CSV et dans une table SQL consultée par un dashboard Power BI.

## Fonctionnement

```
 Sources SQL ──► EXTRACT ──► data/raw/<table>/*.parquet ──► LOAD ──► schéma ODS ──► TRANSFORM ──► procédure stockée
                    │                                         │                        │
                    └──────────────── logs/pipeline_log.csv + dbo.Extraction_Log ◄────┘ ──► Power BI
```

| Phase | Rôle |
|---|---|
| `extract` | Exécute la requête de chaque table active et prévue ce jour, écrit un fichier Parquet ou CSV horodaté |
| `load` | Charge dans le schéma `ODS` **uniquement** le fichier extrait avec succès dans ce run |
| `transform` | Exécute la procédure stockée des tables marquées `"transform": 1` et chargées dans ce run |

Chaque table est traitée dans un bloc `try/except` isolé : une erreur sur une table
n'interrompt pas les autres. Chaque étape est journalisée avec le statut `success`,
`error` ou `skipped` et sa durée (`duration_seconds`).

## Structure

```
config/
  pipeline_config.example.json   # modèle de configuration des tables (aucun code à modifier pour en ajouter)
  .env.example                   # modèle des identifiants de connexion
scripts/
  run_elt.py             # pipeline complet extract → load → transform
  extract_table.py       # première version (extraction seule)
  check_output.py        # vérification d'un fichier extrait
sql/
  create_extraction_log.sql   # table de logs lue par Power BI
data/raw/   logs/             # générés à l'exécution (non versionnés)
```

Le dashboard Power BI (connecté en DirectQuery à `dbo.Extraction_Log`) n'est pas publié,
car il contient la connexion et la structure des tables de l'entreprise.

## Installation

```bash
python -m venv venv
venv\Scripts\activate            # Windows
pip install -r requirements.txt
copy config\.env.example config\.env                                  # puis renseigner les identifiants
copy config\pipeline_config.example.json config\pipeline_config.json  # puis décrire ses tables
```

Prérequis : Python 3.10+, le pilote [ODBC Driver for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server),
et la table `dbo.Extraction_Log` (script `sql/create_extraction_log.sql`).

## Exécution

```bash
python scripts/run_elt.py
```

Pour une exécution quotidienne, planifier cette commande avec le Planificateur de tâches
Windows (ou `cron`).

## Configuration d'une table

Copier `config/pipeline_config.example.json` en `config/pipeline_config.json` (non versionné)
puis y décrire ses propres tables :

```json
{
  "schema": "source_schema",
  "name": "table_a_transformer",
  "query": "SELECT * FROM source_schema.table_a_transformer",
  "is_active": 1,
  "transform": 1,
  "transform_procedure": "dbo.usp_Transform_table_a_transformer",
  "load_mode": "truncate",
  "run_days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
  "format": "parquet",
  "description": "Table chargée puis transformée par une procédure stockée"
}
```

| Clé | Description |
|---|---|
| `schema`, `name` | Table source |
| `query` | Requête d'extraction |
| `is_active` | `1` = traitée, `0` = ignorée (journalisée `skipped`) |
| `run_days` | Jours d'exécution planifiés |
| `format` | `parquet` (recommandé) ou `csv` |
| `load_mode` | `truncate` : vide la table ODS puis recharge (dans une transaction) ; `append` : ajoute les lignes. Défaut : `append` |
| `transform` | `1` = exécute une procédure stockée après le chargement |
| `transform_procedure` | Procédure à appeler. Défaut : `dbo.usp_Transform_<name>` |

Au niveau global, `destination` définit le dossier des fichiers extraits et `load_schema`
le schéma cible du chargement (défaut : `ODS`).

## Sécurité

Les identifiants sont lus depuis `config/.env`, exclu du versionnement par `.gitignore`.
Aucun mot de passe n'est écrit dans le code.

## Perspectives

- Implémentation du schéma DWH (Staging / Curated / DWH)
- Orchestration avec Apache Airflow
- Alertes automatiques en cas d'échec

## Réalisation

**Hayat ARROUBI** — encadrement : Mme Maroua BENLEMSSIAH et M. Yassir RAHMANI (Marjanemall).
