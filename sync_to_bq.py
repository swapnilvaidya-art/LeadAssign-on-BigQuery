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

# --- 2. AUTHENTICATE WITH METABASE ---
session_id = None
login_url = f"{METABASE_URL}/api/session"

try:
    print(f"DEBUG: Authenticating at {login_url}...")
    res = requests.post(login_url, 
                        json={"username": USERNAME, "password": PASSWORD}, 
                        headers={"Content-Type": "application/json"},
                        timeout=15)
    if res.status_code == 200:
        session_id = res.json().get('id')
        print("DEBUG: Login Successful!")
    else:
        print(f"FATAL: Login failed with status {res.status_code}")
        exit(1)
except Exception as e:
    print(f"FATAL: Auth Connection error: {e}")
    exit(1)

# --- 3. PROCESS EACH QUERY ---
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

for q in QUERIES:
    query_url = f"{METABASE_URL}/api/card/{q['card_id']}/query/json"
    print(f"DEBUG: Fetching {q['table_id']} (ID: {q['card_id']})...")
    
    try:
        # Long timeout to prevent "Response ended prematurely"
        data_res = requests.post(query_url, 
                                 headers={"X-Metabase-Session": session_id}, 
                                 timeout=180)
        
        if data_res.status_code == 200:
            df = pd.DataFrame(data_res.json())
            
            # --- A. CONVERT DATES TO TRUE DATE TYPES ---
            # Using lowercase 'stagechange_date' as we discussed
            date_cols = ["lead_created_on", "modified_on", "assign_date", "stagechange_date"]
            for col in date_cols:
                if col in df.columns:
                    # Convert 'March 1, 2026' into real Date objects
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

            # --- B. STRIP SPACES AND FORCE OTHER COLS TO STRING ---
            # This ensures "Session Done" doesn't have hidden spaces
            for col in df.columns:
                if col not in date_cols:
                    df[col] = df[col].astype(str).str.strip()

            # --- C. FIX COLUMN ORDER ---
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
            
            # Reindex ensures columns exist in the right order
            df = df.reindex(columns=column_order)

            # --- D. PUSH TO BIGQUERY ---
            table_path = f"{PROJECT_ID}.{DATASET_ID}.{q['table_id']}"
            # write_disposition="WRITE_TRUNCATE" overwrites the table
            # autodetect=True allows BigQuery to see our new Date types
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
            
            print(f"DEBUG: Uploading {len(df)} rows to {table_path}...")
            client.load_table_from_dataframe(df, table_path, job_config=job_config).result()
            print(f"SUCCESS: Updated {table_path}")
            
            # Brief pause between heavy queries
            time.sleep(5)
            
        else:
            print(f"FAILED: Metabase returned {data_res.status_code} for {q['table_id']}")
            
    except Exception as e:
        print(f"ERROR: Processing {q['table_id']} failed: {str(e)}")
