import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import time

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

# --- 2. AUTHENTICATE ---
session_id = None
login_url = f"{METABASE_URL}/api/session"

try:
    res = requests.post(login_url, 
                        json={"username": USERNAME, "password": PASSWORD}, 
                        headers={"Content-Type": "application/json"},
                        timeout=15)
    session_id = res.json().get('id')
except Exception as e:
    print(f"Auth Failed: {e}")
    exit(1)

# --- 3. PROCESS ---
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

for q in QUERIES:
    query_url = f"{METABASE_URL}/api/card/{q['card_id']}/query/json"
    print(f"DEBUG: Fetching {q['table_id']} (ID: {q['card_id']})...")
    
    try:
        data_res = requests.post(query_url, 
                                 headers={"X-Metabase-Session": session_id}, 
                                 timeout=180) 
        
        if data_res.status_code == 200:
            df = pd.DataFrame(data_res.json())

            # 1. CONVERT DATES (While they are still objects)
            # Using lowercase to match what we'll use in the column_order below
            date_cols = ["lead_created_on", "modified_on", "assign_date", "stagechange_date"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

            # 2. FORCE OTHER COLUMNS TO STRING (to prevent BQ rejection)
            # We skip the date columns so they stay in a format BQ likes
            for col in df.columns:
                if col not in date_cols:
                    df[col] = df[col].astype(str)

            # 3. FIX COLUMN ORDER
            if q['table_id'] == "lead_assignments":
                column_order = [
                    "lead_created_on", "prospect_id", "prospect_email", 
                    "lead_owner", "sales_user_email", "modified_on", 
                    "event", "prospect_stage", "assign_date", "m0_or_not", "course"
                ]
            elif q['table_id'] == "stage_changes":
                column_order = [
                    "lead_created_on", "prospect_id", "prospect_email", 
                    "lead_owner", "sales_user_email", "modified_on", 
                    "event", "previous_stage", "current_stage", 
                    "stagechange_date", "m0_or_not", "course"
                ]
            
            df = df.reindex(columns=column_order)

            # 4. PUSH TO BQ
            table_path = f"{PROJECT_ID}.{DATASET_ID}.{q['table_id']}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(df, table_path, job_config=job_config).result()
            print(f"SUCCESS: Updated {table_path}")
            
            time.sleep(5) 
            
        else:
            print(f"FAILED: {q['table_id']} returned {data_res.status_code}")
            
    except Exception as e:
        print(f"ERROR: {q['table_id']} failed with: {str(e)}")
