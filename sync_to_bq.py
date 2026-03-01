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

QUERIES = [
    {"card_id": "9600", "table_id": "lead_assignments"},
    {"card_id": "9607", "table_id": "stage_changes"}
]

# --- 2. AUTHENTICATE WITH METABASE ---
session_id = None
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
    except Exception as e:
        print(f"DEBUG: Connection error at {base}: {e}")

if not session_id:
    print("FATAL: Could not get a Session ID.")
    exit(1)

# --- 3. PROCESS EACH QUERY ---
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

for q in QUERIES:
    # --- RESTORED FETCH LOGIC ---
    query_url = f"{final_base_url}/api/card/{q['card_id']}/query/json"
    print(f"DEBUG: Fetching Query {q['card_id']} from {query_url}")
    
    data_res = requests.post(query_url, headers={"X-Metabase-Session": session_id})
    
    if data_res.status_code == 200:
        df = pd.DataFrame(data_res.json())
        
        # --- FIX COLUMN ORDER ---
        if q['table_id'] == "lead_assignments":
            column_order = [
                "lead_created_on", "prospect_id", "prospect_email", 
                "lead_owner", "sales_user_email", "modified_on", 
                "event", "prospect_stage", "assign_date", "course"
            ]
            df = df[[c for c in column_order if c in df.columns]]
            
        elif q['table_id'] == "stage_changes":
            column_order = [
                "lead_created_on", "prospect_id", "prospect_email", 
                "lead_owner", "sales_user_email", "modified_on", 
                "event", "previous_stage", "current_stage", 
                "StageChange_date", "M0_or_Not", "course"
            ]
            df = df[[c for c in column_order if c in df.columns]]

        # --- PUSH TO BIGQUERY ---
        table_path = f"{PROJECT_ID}.{DATASET_ID}.{q['table_id']}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_path, job_config=job_config).result()
        print(f"Successfully updated {table_path} with fixed column order!")
    else:
        print(f"Failed to fetch Query {q['card_id']}: {data_res.status_code}")
