import requests
import pandas as pd
from google.cloud import bigquery
import os

# --- PARAMÈTRES ---
PROJECT_ID = "my-project-florent-bq"
DATASET_ID = "raw_data_fr_carburant"
TABLE_ID   = "fuel_api_prices_raw"

# URL de l'API en format JSON
API_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"

def ingest_to_bigquery():
    # Initialisation du client (automatique si authentifié via GitHub Action)
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    print(f"Téléchargement des données JSON...")
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        data = response.json()
        
        # Transformation en DataFrame pour un export facile
        df = pd.DataFrame(data)
        
        # Configuration de l'upload BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # On écrase la table
            autodetect=True,                    # BQ va gérer les colonnes JSON automatiquement
        )

        print(f"Chargement dans BigQuery ({table_ref})...")
        # Ingestion directe depuis le DataFrame
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        print(f"Succès ! {len(df)} objets JSON insérés dans {TABLE_ID}.")
    else:
        print(f"Erreur API : {response.status_code}")

if __name__ == "__main__":
    ingest_to_bigquery()
