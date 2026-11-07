"""Affiche le contenu d'un fichier extrait.

Usage : python scripts/check_output.py data/raw/<table>/<fichier>.parquet
"""
import sys
import pandas as pd

if len(sys.argv) != 2:
    sys.exit(__doc__)

path = sys.argv[1]
df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
print(df)
print(f"\nNombre de lignes : {len(df)}")
print(f"Colonnes : {list(df.columns)}")
