import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- 1. SETUP ---
METABASE_URL = os.getenv("METABASE_URL").strip().rstrip('/')
USERNAME = os.getenv("USERNAME").strip()
PASSWORD = os.getenv("SWAPNIL_SECRET_KEY").strip()
CARD_ID = "9600"

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# --- 2. THE "SMART AUTH" ---
# Try the standard path first
login_url = f"{METABASE_URL}/api/session"
print(f"DEBUG: Trying primary login at {login_url}")

auth_payload = {"username": USERNAME, "password": PASSWORD}
headers = {"Content-Type": "application/json"}

session_response = requests.post(login_url, json=auth_payload, headers=headers)

# FALLBACK: If 404, try adding a trailing slash (Metabase quirk)
if session_response.status_code == 404:
    print("DEBUG: Primary failed (404), trying fallback with trailing slash...")
    login_url = f"{METABASE_URL}/api/session/"
    session_response = requests.post(login_url, json=auth_payload, headers=headers)

if session_response.status_code != 200:
    print(f"FAILED. Code: {session_response.status_code} | Body: {session_response.text}")
    exit(1)

session_id = session_response.json().get('id')

# --- 3. FETCH & UPLOAD ---
query_url = f"{METABASE_URL}/api/card/{CARD_ID}/query/json"
data_response = requests.post(query_url, headers={"X-Metabase-Session": session_id})
df = pd.DataFrame(data_response.json())

info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
client.load_table_from_dataframe(df, full_table_path, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()

print(f"Success! Data updated in {full_table_path}")
