import os
import json
import csv
import time
from datetime import datetime
import pyodbc
import pandas as pd
from dotenv import load_dotenv


def extract_table():
    # 1. Chemins absolus
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(BASE_DIR, "..", "config", ".env")
    config_path = os.path.join(BASE_DIR, "..", "config", "pipeline_config.json")
    log_path = os.path.join(BASE_DIR, "..", "logs", "pipeline_log.csv")

    # 2. Charger les credentials depuis .env
    load_dotenv(dotenv_path=env_path)
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")

    # 3. Charger la config JSON
    with open(config_path) as f:
        config = json.load(f)

    tables = config["tables"]
    destination = config["destination"]

    # 4. Se connecter à la base
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={db_user};"
        f"PWD={db_password}"
    )
    conn = pyodbc.connect(conn_str)

    # 5. Jour et date actuels
    today_name = datetime.now().strftime("%A").lower()
    today_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    executed_by = os.getlogin()

    # 6. Constante : quelle phase ELT exécute ce script
    PHASE = "extract"

    # 7. Fonction interne pour écrire une ligne de log
    def write_log(schema, table_name, status, rows_extracted, duration_seconds,
                   file_format, file_path, query_used, message,
                   phase, filename, nombre_ligne):
        # --- Partie 1 : écriture dans le CSV ---
        log_exists = os.path.exists(log_path)

        with open(log_path, mode="a", newline="", encoding="utf-8") as log_file:
            writer = csv.writer(log_file)

            if not log_exists:
                writer.writerow([
                    "execution_date", "schema", "table_name", "status",
                    "rows_extracted", "duration_seconds", "file_format",
                    "file_path", "query_used", "executed_by", "message",
                    "phase", "filename", "nombre_ligne"
                ])

            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                schema,
                table_name,
                status,
                rows_extracted,
                duration_seconds,
                file_format,
                file_path,
                query_used,
                executed_by,
                message,
                phase,
                filename,
                nombre_ligne
            ])

        # --- Partie 2 : écriture dans la table SQL ---
        cursor = conn.cursor()

        now = datetime.now()

        insert_query = """
            INSERT INTO dbo.Extraction_Log
            (execution_date, schema_name, table_name, status,
             rows_extracted, duration_seconds, file_format, file_path,
             query_used, executed_by, message, phase, filename, nombre_ligne)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(insert_query, (
            now,
            schema,
            table_name,
            status,
            int(rows_extracted) if rows_extracted != "" else None,
            float(duration_seconds) if duration_seconds != "" else None,
            file_format,
            file_path,
            query_used,
            executed_by,
            message,
            phase,
            filename,
            int(nombre_ligne) if nombre_ligne != "" else None
        ))
        conn.commit()
        cursor.close()

    # 8. Boucle sur chaque table
    for table_config in tables:
        schema = table_config["schema"]
        name = table_config["name"]
        query = table_config["query"]
        is_active = table_config["is_active"]
        run_days = table_config["run_days"]
        file_format = table_config["format"]

        full_table_name = f"{schema}.{name}"

        if is_active == 0:
            print(f"Ignorée (inactive) : {full_table_name}")
            write_log(schema, name, "skipped", "", "", file_format, "", query,
                       "Ignorée (inactive)", PHASE, "", "")
            continue

        if today_name not in run_days:
            print(f"Ignorée (pas prévue aujourd'hui) : {full_table_name}")
            write_log(schema, name, "skipped", "", "", file_format, "", query,
                       "Ignorée (pas prévue aujourd'hui)", PHASE, "", "")
            continue

        start_time = time.time()

        try:
            df = pd.read_sql(query, conn)

            table_folder = os.path.join(BASE_DIR, "..", destination, name)
            os.makedirs(table_folder, exist_ok=True)

            filename = f"{name}_{today_date}.{file_format}"
            filepath = os.path.join(table_folder, filename)

            if file_format == "parquet":
                df.to_parquet(filepath, index=False)
            elif file_format == "csv":
                df.to_csv(filepath, index=False)

            duration = round(time.time() - start_time, 2)

            print(f"Extraction terminée : {filepath} ({len(df)} lignes)")
            write_log(schema, name, "success", len(df), duration, file_format,
                       filepath, query, "OK", PHASE, filename, len(df))

        except Exception as e:
            duration = round(time.time() - start_time, 2)
            print(f"Erreur sur {full_table_name} : {e}")
            write_log(schema, name, "error", "", duration, file_format, "", query,
                       str(e), PHASE, "", "")

    conn.close()


if __name__ == "__main__":
    extract_table()