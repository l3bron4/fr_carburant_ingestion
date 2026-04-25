import requests
import pandas as pd
from google.cloud import bigquery
import io
import sys

# --- PARAMÈTRES ---
PROJECT_ID = "my-project-florent-bq"
DATASET_ID = "raw_data_fr_carburant"
TABLE_ID   = "fuel_api_prices_raw"

# URL de l'API en format JSON
API_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"

def ingest_to_bigquery():
    print("--- DÉBUT DE L'INGESTION ---", flush=True)
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        print(f"Appel de l'API : {API_URL}...", flush=True)
        response = requests.get(API_URL, timeout=60)
        
        if response.status_code != 200:
            print(f"ERREUR API : Status {response.status_code}", flush=True)
            return

        data = response.json()
        nb_lignes = len(data)
        print(f"Données reçues : {nb_lignes} lignes trouvées.", flush=True)

        if nb_lignes == 0:
            print("Attention : L'API a renvoyé une liste vide !", flush=True)
            return

        # Transformation
        df = pd.DataFrame(data)
        
        # On force la conversion de tout en string si c'est du brut pour dbt, 
        # ça évite les erreurs de type au chargement
        df = df.astype(str)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        print(f"Envoi vers BigQuery : {table_ref}...", flush=True)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        
        # On attend et on check les erreurs spécifiques au job BQ
        result = job.result() 
        
        print(f"TERMINÉ : {nb_lignes} lignes insérées avec succès.", flush=True)

    except Exception as e:
        print(f"ERREUR CRITIQUE : {str(e)}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    ingest_to_bigquery()
