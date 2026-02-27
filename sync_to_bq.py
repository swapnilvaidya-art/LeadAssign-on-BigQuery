import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- 1. SETUP FROM YOUR EXISTING KEYS ---
# These names match the ones you provided exactly
METABASE_URL = os.getenv("METABASE_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("SWAPNIL_SECRET_KEY")
CARD_ID = "9600" # Extracted from your link

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# --- 2. AUTHENTICATE WITH METABASE ---
auth_data = {"username": USERNAME, "password": PASSWORD}
session_response = requests.post(f"{METABASE_URL}/api/session", json=auth_data)

# Check if login was successful
if session_response.status_code != 200:
    print(f"Login failed! Response: {session_response.text}")
    exit(1)

session_id = session_response.json().get('id')

# --- 3. FETCH DATA FROM METABASE ---
headers = {"X-Metabase-Session": session_id}
query_url = f"{METABASE_URL}/api/card/{CARD_ID}/query/json"
data_response = requests.post(query_url, headers=headers)

# Check if query was successful
if data_response.status_code != 200:
    print(f"Query failed! Response: {data_response.text}")
    exit(1)

rows = data_response.json()

# --- 3. FETCH DATA FROM METABASE ---
headers = {"X-Metabase-Session": session_id}
# This hits the specific "Lead Assignment Query" (9600)
query_url = f"{METABASE_URL}/api/card/{CARD_ID}/query/json"
data_response = requests.post(query_url, headers=headers)
rows = data_response.json()

# Convert to a format BigQuery understands
df = pd.DataFrame(rows)

# --- 4. UPLOAD TO BIGQUERY ---
# Use your newly created service account JSON
info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# This will overwrite the table with the fresh query output each time
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(df, full_table_path, job_config=job_config)
job.result() # Waits for the upload to finish

print(f"Successfully updated {full_table_path}!")
