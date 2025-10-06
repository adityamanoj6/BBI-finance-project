#!/usr/bin/env python3
import os
import time
import pandas as pd
import snowflake.connector
import requests
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# -------------------------
# Start timer
# -------------------------
overall_start = time.time()

# -------------------------
# Load private key
# -------------------------
with open("/home/manoj/airflow/dags/snowflake_private_key.pem", "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=None,
        backend=default_backend()
    )

private_key = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# -------------------------
# Snowflake connect
# -------------------------
conn = snowflake.connector.connect(
    user="AUTOMATION_SERVICE",
    account="HWHWKHQ-OG54846",
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="FINANCE_PROJECT",
    schema="ANALYTICS",
    role="INTERN_ANALYST",
    private_key=private_key
)
cursor = conn.cursor()

# -------------------------
# Read table
# -------------------------
read_start = time.time()
query = """
SELECT CustomerId, Name, CreditScore, Geography, Gender, Age, Tenure,
       Balance, NumOfProducts, HasCrCard, IsActiveMember,
       EstimatedSalary, Exited
FROM BANK_CUSTOMER_CHURN
"""
df = pd.read_sql(query, conn)
df.rename(columns={
    'CUSTOMERID': 'CustomerId',
    'NAME': 'Name',
    'CREDITSCORE': 'CreditScore',
    'GEOGRAPHY': 'Geography',
    'GENDER': 'Gender',
    'AGE': 'Age',
    'TENURE': 'Tenure',
    'BALANCE': 'Balance',
    'NUMOFPRODUCTS': 'NumOfProducts',
    'HASCRCARD': 'HasCrCard',
    'ISACTIVEMEMBER': 'IsActiveMember',
    'ESTIMATEDSALARY': 'EstimatedSalary',
    'EXITED': 'Exited'
}, inplace=True)

read_end = time.time()

# -------------------------
# Helpers
# -------------------------
def safe_value(value):
    try:
        if pd.isna(value):
            return None
        return value
    except Exception:
        return None

def create_prompt(row, row_id):
    customer = {
        "row_id": int(row_id),
        "CustomerId": safe_value(row['CustomerId']),
        "Name": str(row['Name']),
        "CreditScore": safe_value(row['CreditScore']),
        "Geography": str(row['Geography']),
        "Gender": str(row['Gender']),
        "Age": safe_value(row['Age']),
        "Tenure": safe_value(row['Tenure']),
        "Balance": safe_value(row['Balance']),
        "NumOfProducts": safe_value(row['NumOfProducts']),
        "HasCrCard": bool(row['HasCrCard']),
        "IsActiveMember": bool(row['IsActiveMember']),
        "EstimatedSalary": safe_value(row['EstimatedSalary']),
        "Exited": bool(row['Exited'])
    }

    prompt = (
        "You are a financial analyst. Analyze this bank customer churn record "
        "and return ONLY valid JSON.\n\n"
        "Return an object with:\n"
        "  - customerid (same as input)\n"
        "  - row_id (input row number)\n"
        "  - summary (1 sentence on churn likelihood / retention risk)\n"
        "  - risk_category (Low / Medium / High churn risk)\n"
        "  - recommendation (1 practical action to improve retention)\n\n"
        f"Input data:\n{json.dumps(customer, indent=2)}\n\n"
        "Output JSON object:"
    )
    return prompt

# -------------------------
# Call Ollama
# -------------------------
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL = "phi:latest"  # replace with your model name

def call_ollama_single(row_id, row):
    prompt = create_prompt(row, row_id)
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        llm_response = data.get("message", {}).get("content", "").strip()
        print(f"✅ Row {row_id} processed")
        return (row_id, str(row['CustomerId']), str(row['Name']), prompt, llm_response)
    except Exception as e:
        print(f"❌ Row {row_id} failed: {e}")
        return (row_id, str(row['CustomerId']), str(row['Name']), prompt, f"ERROR: {e}")

# -------------------------
# Sequential execution
# -------------------------
results = []
processing_start = time.time()
for idx, row in df.iterrows():
    results.append(call_ollama_single(idx, row))
processing_end = time.time()

# -------------------------
# Store results in Snowflake
# -------------------------
store_start = time.time()
cursor.execute("""
CREATE TABLE IF NOT EXISTS BANK_CUSTOMER_CHURN_LLM_RESULTS (
   ROW_NUM INTEGER,
   CUSTOMERID STRING,
   NAME STRING,
   PROMPT STRING,
   LLM_RESPONSE STRING,
   CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

cursor.executemany("""
INSERT INTO BANK_CUSTOMER_CHURN_LLM_RESULTS (ROW_NUM, CUSTOMERID, NAME, PROMPT, LLM_RESPONSE)
VALUES (%s, %s, %s, %s, %s)
""", results)
conn.commit()
store_end = time.time()

# -------------------------
# Close connection
# -------------------------
cursor.close()
conn.close()

overall_end = time.time()

# -------------------------
# Time summary
# -------------------------
print("\n⏱️ Time Report:")
print(f" - Read from Snowflake: {read_end - read_start:.2f} sec")
print(f" - LLM processing: {processing_end - processing_start:.2f} sec")
print(f" - Store results: {store_end - store_start:.2f} sec")
print(f" - TOTAL runtime: {overall_end - overall_start:.2f} sec")

print("✅ Done. Check BANK_CUSTOMER_CHURN_LLM_RESULTS in Snowflake.")
