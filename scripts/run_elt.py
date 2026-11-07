"""
run_elt.py — Framework Python générique d'ELT (Extract → Load → Transform)

    1. EXTRACT   : lit chaque table source et l'écrit en Parquet/CSV (data/raw/<table>/)
    2. LOAD      : charge le fichier extrait DANS CE RUN vers le schéma ODS
    3. TRANSFORM : exécute la procédure stockée des tables marquées "transform": 1

Tout est piloté par config/pipeline_config.json ; les credentials sont lus
depuis config/.env (jamais versionné). Chaque étape est tracée dans
logs/pipeline_log.csv et dans la table SQL dbo.Extraction_Log.
"""

import os
import csv
import json
import time
import getpass
from datetime import datetime
from urllib.parse import quote_plus

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, "config", ".env")
CONFIG_PATH = os.path.join(PROJECT_DIR, "config", "pipeline_config.json")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "pipeline_log.csv")

LOG_COLUMNS = [
    "execution_date", "schema", "table_name", "status",
    "rows_extracted", "duration_seconds", "file_format",
    "file_path", "query_used", "executed_by", "message",
    "phase", "filename", "nombre_ligne",
]


def _to_int(value):
    return int(value) if value not in ("", None) else None


def _to_float(value):
    return float(value) if value not in ("", None) else None


def run_elt():
    # 1. Credentials (.env)
    load_dotenv(dotenv_path=ENV_PATH)
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")

    missing = [k for k, v in {
        "DB_SERVER": server, "DB_NAME": database, "DB_USER": db_user,
        "DB_PASSWORD": db_password, "DB_DRIVER": driver,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"Variables manquantes dans {ENV_PATH} : {', '.join(missing)} "
            f"(voir config/.env.example)"
        )

    # 2. Configuration JSON (UTF-8 : les descriptions contiennent des accents)
    if not os.path.exists(CONFIG_PATH):
        raise RuntimeError(
            f"Configuration introuvable : {CONFIG_PATH} "
            f"(copier config/pipeline_config.example.json puis l'adapter)"
        )
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    tables = config["tables"]
    destination = config.get("destination", "data/raw/")
    load_schema = config.get("load_schema", "ODS")

    # 3. Connexions : pyodbc pour les lectures/logs, SQLAlchemy pour to_sql / EXEC
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={db_user};"
        f"PWD={db_password}"
    )
    conn = pyodbc.connect(conn_str)
    # quote_plus encode correctement les caractères spéciaux du mot de passe
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")

    today_name = datetime.now().strftime("%A").lower()
    today_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    executed_by = getpass.getuser()  # os.getlogin() échoue en tâche planifiée

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

    # 4. Journalisation (CSV + SQL), partagée par les 3 phases
    def write_log(schema, table_name, status, rows_extracted, duration_seconds,
                  file_format, file_path, query_used, message,
                  phase, filename, nombre_ligne):
        log_exists = os.path.exists(LOG_PATH)
        with open(LOG_PATH, mode="a", newline="", encoding="utf-8") as log_file:
            writer = csv.writer(log_file)
            if not log_exists:
                writer.writerow(LOG_COLUMNS)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                schema, table_name, status, rows_extracted, duration_seconds,
                file_format, file_path, query_used, executed_by, message,
                phase, filename, nombre_ligne,
            ])

        # Un échec d'écriture du log SQL ne doit pas arrêter le pipeline :
        # la ligne reste disponible dans le CSV.
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO dbo.Extraction_Log
                (execution_date, schema_name, table_name, status,
                 rows_extracted, duration_seconds, file_format, file_path,
                 query_used, executed_by, message, phase, filename, nombre_ligne)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(), schema, table_name, status,
                    _to_int(rows_extracted), _to_float(duration_seconds),
                    file_format, file_path, query_used, executed_by, message,
                    phase, filename, _to_int(nombre_ligne),
                ),
            )
            conn.commit()
            cursor.close()
        except Exception as e:
            print(f"[log] Avertissement : écriture SQL impossible ({e}), log CSV conservé")

    def skip(phase, table_config, reason, query=""):
        name = table_config["name"]
        print(f"[{phase}] Ignorée ({reason}) : {table_config['schema']}.{name}")
        write_log(table_config["schema"], name, "skipped", "", "",
                  table_config.get("format", ""), "", query, f"Ignorée ({reason})",
                  phase, "", "")

    # 5. EXTRACT — renvoie {nom_table: chemin du fichier créé dans ce run}
    def extract():
        extracted = {}
        for table_config in tables:
            schema = table_config["schema"]
            name = table_config["name"]
            query = table_config["query"]
            file_format = table_config.get("format", "parquet")

            if table_config.get("is_active", 1) == 0:
                skip("extract", table_config, "inactive", query)
                continue
            if today_name not in table_config.get("run_days", []):
                skip("extract", table_config, "pas prévue aujourd'hui", query)
                continue

            start_time = time.time()
            try:
                df = pd.read_sql(query, conn)

                table_folder = os.path.normpath(os.path.join(PROJECT_DIR, destination, name))
                os.makedirs(table_folder, exist_ok=True)
                filename = f"{name}_{today_date}.{file_format}"
                filepath = os.path.join(table_folder, filename)

                if file_format == "parquet":
                    df.to_parquet(filepath, index=False)
                elif file_format == "csv":
                    df.to_csv(filepath, index=False)
                else:
                    raise ValueError(f"Format non supporté : {file_format}")

                duration = round(time.time() - start_time, 2)
                extracted[name] = filepath
                print(f"[extract] Terminé : {filepath} ({len(df)} lignes)")
                write_log(schema, name, "success", len(df), duration, file_format,
                          filepath, query, "OK", "extract", filename, len(df))

            except Exception as e:
                duration = round(time.time() - start_time, 2)
                print(f"[extract] Erreur sur {schema}.{name} : {e}")
                write_log(schema, name, "error", "", duration, file_format, "", query,
                          str(e), "extract", "", "")
        return extracted

    # 6. LOAD — ne charge QUE les fichiers extraits avec succès dans ce run
    #    (évite de recharger un ancien fichier et de créer des doublons)
    def load(extracted):
        loaded = set()
        for table_config in tables:
            schema = table_config["schema"]
            name = table_config["name"]
            file_format = table_config.get("format", "parquet")
            load_mode = table_config.get("load_mode", "append")

            if table_config.get("is_active", 1) == 0:
                skip("load", table_config, "inactive")
                continue
            if name not in extracted:
                skip("load", table_config, "non extraite dans ce run")
                continue

            start_time = time.time()
            filepath = extracted[name]
            try:
                if file_format == "parquet":
                    df = pd.read_parquet(filepath)
                else:
                    df = pd.read_csv(filepath)

                # Une seule transaction : si l'insertion échoue, le vidage est annulé
                with engine.begin() as sql_conn:
                    if load_mode == "truncate" and inspect(sql_conn).has_table(name, schema=load_schema):
                        sql_conn.execute(text(f"DELETE FROM [{load_schema}].[{name}]"))
                    df.to_sql(name=name, con=sql_conn, schema=load_schema,
                              if_exists="append", index=False)

                duration = round(time.time() - start_time, 2)
                filename = os.path.basename(filepath)
                loaded.add(name)
                print(f"[load] Terminé : {load_schema}.{name} ({len(df)} lignes, mode {load_mode})")
                write_log(schema, name, "success", len(df), duration, file_format,
                          filepath, "", f"OK ({load_mode})", "load", filename, len(df))

            except Exception as e:
                duration = round(time.time() - start_time, 2)
                print(f"[load] Erreur sur {name} : {e}")
                write_log(schema, name, "error", "", duration, file_format, filepath, "",
                          str(e), "load", "", "")
        return loaded

    # 7. TRANSFORM — uniquement les tables "transform": 1 chargées dans ce run
    def transform(loaded):
        for table_config in tables:
            schema = table_config["schema"]
            name = table_config["name"]
            file_format = table_config.get("format", "parquet")

            if table_config.get("transform", 0) != 1:
                continue  # pas de transformation prévue pour cette table
            procedure = table_config.get("transform_procedure", f"dbo.usp_Transform_{name}")
            exec_query = f"EXEC {procedure}"

            if table_config.get("is_active", 1) == 0:
                skip("transform", table_config, "inactive", exec_query)
                continue
            if name not in loaded:
                skip("transform", table_config, "non chargée dans ce run", exec_query)
                continue

            start_time = time.time()
            try:
                with engine.begin() as sql_conn:
                    result = sql_conn.execute(text(exec_query))
                    row = result.fetchone() if result.returns_rows else None
                    rows_transformed = row[0] if row else 0

                duration = round(time.time() - start_time, 2)
                print(f"[transform] Terminé : {procedure} ({rows_transformed} lignes)")
                write_log(schema, name, "success", rows_transformed, duration,
                          file_format, "", exec_query, "OK", "transform", "",
                          rows_transformed)

            except Exception as e:
                duration = round(time.time() - start_time, 2)
                print(f"[transform] Erreur sur {name} : {e}")
                write_log(schema, name, "error", "", duration, file_format, "",
                          exec_query, str(e), "transform", "", "")

    # 8. Enchaînement du processus complet
    try:
        extracted = extract()
        loaded = load(extracted)
        transform(loaded)
    finally:
        conn.close()
        engine.dispose()


if __name__ == "__main__":
    run_elt()
