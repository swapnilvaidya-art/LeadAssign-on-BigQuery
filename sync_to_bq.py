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
session_id = None
# Newton School usually needs the /metabase prefix. We try that first.
possible_urls = [f"{METABASE_URL}/metabase", METABASE_URL]
final_base_url = METABASE_URL

for base in possible_urls:
    login_url = f"{base}/api/session"
    print(f"DEBUG: Trying login at {login_url}")
    try:
        res = requests.post(login_url, 
                            json={"username": USERNAME, "password": PASSWORD}, 
                            headers={"Content-Type": "application/json"},
                            timeout=15)
        if res.status_code == 200:
            session_id = res.json().get('id')
            final_base_url = base
            print("DEBUG: Login Successful!")
            break
        else:
            print(f"DEBUG: Path {login_url} returned {res.status_code}")
    except Exception as e:
        print(f"DEBUG: Connection error at {base}: {e}")

if not session_id:
    print("FATAL: Could not get a Session ID from Metabase.")
    exit(1)

# --- 3. PROCESS EACH QUERY ---
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

for q in QUERIES:
    # Use the base URL that actually worked for login
    query_url = f"{final_base_url}/api/card/{q['card_id']}/query/json"
    print(f"DEBUG: Fetching Query {q['card_id']} from {query_url}")
    
    data_res = requests.post(query_url, headers={"X-Metabase-Session": session_id})
    
    if data_res.status_code == 200:
        # Check if response is empty string
        if not data_res.text:
            print(f"ERROR: Query {q['card_id']} returned no data.")
            continue
            
        df = pd.DataFrame(data_res.json())
        table_path = f"{PROJECT_ID}.{DATASET_ID}.{q['table_id']}"
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_path, job_config=job_config).result()
        print(f"Successfully updated {table_path}")
    else:
        print(f"Failed to fetch Query {q['card_id']}: {data_res.status_code} - {data_res.text}")
