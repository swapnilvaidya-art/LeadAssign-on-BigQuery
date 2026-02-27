import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- 1. CLEAN THE INPUTS ---
# This ensures no accidental spaces or slashes break the link
METABASE_URL = os.getenv("METABASE_URL").strip().rstrip('/')
USERNAME = os.getenv("USERNAME").strip()
PASSWORD = os.getenv("SWAPNIL_SECRET_KEY").strip()
CARD_ID = "9600"

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# --- 2. AUTHENTICATE ---
# Metabase expects the login at /api/session
login_url = f"{METABASE_URL}/api/session"
print(f"DEBUG: Attempting login at {login_url}") # This will show us the truth in the logs

auth_data = {"username": USERNAME, "password": PASSWORD}
session_response = requests.post(login_url, json=auth_data)

if session_response.status_code != 200:
    print(f"Login failed! Code: {session_response.status_code}")
    print(f"Response Body: {session_response.text}")
    exit(1)

session_id = session_response.json().get('id')

# --- 3. FETCH DATA ---
headers = {"X-Metabase-Session": session_id}
query_url = f"{METABASE_URL}/api/card/{CARD_ID}/query/json"
data_response = requests.post(query_url, headers=headers)

if data_response.status_code != 200:
    print(f"Query failed! Response: {data_response.text}")
    exit(1)

df = pd.DataFrame(data_response.json())

# --- 4. UPLOAD TO BIGQUERY ---
try:
    info = json.loads(SERVICE_ACCOUNT_JSON)
    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(df, full_table_path, job_config=job_config)
    job.result() 
    print(f"Success! Data updated in {full_table_path}")
except Exception as e:
    print(f"BigQuery Error: {str(e)}")
    exit(1)
