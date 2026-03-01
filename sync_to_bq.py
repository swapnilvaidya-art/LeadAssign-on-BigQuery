import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- 1. CONFIGURATION ---
METABASE_URL = os.getenv("METABASE_URL").strip().rstrip('/')
USERNAME = os.getenv("USERNAME").strip()
PASSWORD = os.getenv("SWAPNIL_SECRET_KEY").strip()

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# List your queries and their target tables here
QUERIES = [
    {"card_id": "9600", "table_id": "lead_assignments"},
    {"card_id": "9607", "table_id": "stage_changes"}
]

# --- 2. AUTHENTICATE WITH METABASE ---
login_url = f"{METABASE_URL}/metabase/api/session"
print(f"Attempting login at {login_url}")

try:
    auth_res = requests.post(login_url, 
                            json={"username": USERNAME, "password": PASSWORD}, 
                            headers={"Content-Type": "application/json"},
                            timeout=15)
    if auth_res.status_code != 200:
        # Fallback if /metabase prefix isn't needed
        login_url = f"{METABASE_URL}/api/session"
        auth_res = requests.post(login_url, json={"username": USERNAME, "password": PASSWORD}, headers={"Content-Type": "application/json"})
    
    session_id = auth_res.json().get('id')
except Exception as e:
    print(f"Auth Error: {e}")
    exit(1)

# --- 3. PROCESS EACH QUERY ---
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

for q in QUERIES:
    print(f"Updating Table: {q['table_id']}...")
    
    # Use the correct base URL based on which login worked
    base_api = login_url.replace('/api/session', '')
    query_url = f"{base_api}/api/card/{q['card_id']}/query/json"
    
    data_res = requests.post(query_url, headers={"X-Metabase-Session": session_id})
    
    if data_res.status_code == 200:
        df = pd.DataFrame(data_res.json())
        table_path = f"{PROJECT_ID}.{DATASET_ID}.{q['table_id']}"
        
        # WRITE_TRUNCATE replaces the table with fresh data
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_path, job_config=job_config).result()
        print(f"Successfully updated {table_path}")
    else:
        print(f"Failed to fetch Query {q['card_id']}: {data_res.text}")
