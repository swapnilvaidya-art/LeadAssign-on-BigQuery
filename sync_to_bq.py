import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- 1. SETUP ---
# .strip().rstrip('/') removes accidental spaces or extra slashes
METABASE_URL = os.getenv("METABASE_URL").strip().rstrip('/')
USERNAME = os.getenv("USERNAME").strip()
PASSWORD = os.getenv("SWAPNIL_SECRET_KEY").strip()
CARD_ID = "9600"

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# --- 2. AUTHENTICATE ---
auth_data = {"username": USERNAME, "password": PASSWORD}
# This now builds the URL perfectly regardless of secret formatting
session_response = requests.post(f"{METABASE_URL}/api/session", json=auth_data)

if session_response.status_code != 200:
    print(f"Login failed! Response: {session_response.text}")
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
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(df, full_table_path, job_config=job_config)
job.result() 

print(f"Success! Data is now in {full_table_path}")
