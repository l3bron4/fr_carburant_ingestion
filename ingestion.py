import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import os

# --- PARAMÈTRES À CONFIGURER ---
PROJECT_ID = "my-project-florent-bq" # Remplace par ton ID projet BigQuery
DATASET_ID = "raw_data_fr_carburant"
TABLE_ID   = "fuel_api_prices_raw"
# URL de l'API (export CSV pour une ingestion facile vers BQ)
API_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/csv"

def ingest_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print(f"Téléchargement des données...")
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        # On utilise pandas pour lire le flux CSV
        df = pd.read_csv(io.StringIO(response.text), sep=';')
        
        # Configuration de l'upload BigQuery
        job_config = bigquery.LoadJobConfig(
            # On écrase la table à chaque fois (Stratégie Full Refresh)
            write_disposition="WRITE_TRUNCATE", 
            autodetect=True, # Laisse BQ deviner le schéma pour le brut
        )

        print(f"Chargement dans BigQuery ({table_ref})...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Attend la fin du job
        
        print(f"Succès ! {len(df)} lignes insérées dans {TABLE_ID}.")
    else:
        print(f"Erreur API : {response.status_code}")

if __name__ == "__main__":
    ingest_to_bigquery()
