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
query = """
SELECT CREDITCUSTID,NAME,LIMIT_BAL, SEX, EDUCATION, MARRIAGE, AGE, PAY_0, PAY_2,
       BILL_AMT1, BILL_AMT2, PAY_AMT1, PAY_AMT2, DEFAULT_PAYMENT_NEXT_MONTH
FROM UCI_CREDIT_DEFAULT
"""
read_start = time.time()
df = pd.read_sql(query, conn)
read_end = time.time()
print(f"📥 Read {len(df)} rows from UCI_CREDIT_DEFAULT in {read_end - read_start:.2f} sec")

# -------------------------
# Ollama config
# -------------------------
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL = "phi:latest"

# -------------------------
# Wait for Ollama
# -------------------------
print("⏳ Checking if Ollama server is ready...")
for i in range(10):
    try:
        resp = requests.get("http://127.0.0.1:11434/api/tags", timeout=5)
        if resp.status_code == 200 and resp.json().get("models") is not None:
            print("✅ Ollama server is ready!")
            break
    except Exception:
        pass
    time.sleep(1)

# -------------------------
# Helpers
# -------------------------
def safe_int(value):
    try:
        if pd.isna(value):
            return None
        return int(value)
    except Exception:
        return None

def create_prompt(row, row_id):
    customer = {
        "row_id": int(row_id),
        "CREDITCUSTID": str(row['CREDITCUSTID']),
        "NAME": str(row['NAME']),
        "LIMIT_BAL": safe_int(row['LIMIT_BAL']),
        "SEX": safe_int(row['SEX']),
        "EDUCATION": safe_int(row['EDUCATION']),
        "MARRIAGE": safe_int(row['MARRIAGE']),
        "AGE": safe_int(row['AGE']),
        "PAY_0": safe_int(row['PAY_0']),
        "PAY_2": safe_int(row['PAY_2']),
        "BILL_AMT1": safe_int(row['BILL_AMT1']),
        "BILL_AMT2": safe_int(row['BILL_AMT2']),
        "PAY_AMT1": safe_int(row['PAY_AMT1']),
        "PAY_AMT2": safe_int(row['PAY_AMT2']),
        "DEFAULT_NEXT_MONTH": safe_int(row['DEFAULT_PAYMENT_NEXT_MONTH'])
    }

    prompt = (
        "You are a financial analyst. Analyze this customer record and return ONLY valid JSON.\n"
        "Do not include explanations or text outside JSON.\n\n"
        "Return an object with:\n"
        "  - creditcustomerid (same as input)\n"
        "  - row_id (input row number)\n"
        "  - summary (1 sentence credit risk summary)\n"
        "  - risk_category (Low / Medium / High)\n"
        "  - recommendation (1 practical recommendation)\n\n"
        f"Input data:\n{json.dumps(customer, indent=2)}\n\n"
        "Output JSON object:"
    )
    return prompt


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
        return (row_id, str(row['CREDITCUSTID']), str(row['NAME']), prompt, llm_response)
    except Exception as e:
        print(f"❌ Row {row_id} failed: {e}")
        return (row_id, str(row['CREDITCUSTID']), str(row['NAME']), prompt, f"ERROR: {e}")


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
CREATE TABLE IF NOT EXISTS UCI_CREDIT_DEFAULT_LLM_RESULTS (
   ROW_NUM INTEGER,
   CREDITCUSTID STRING,
   NAME STRING,
   PROMPT STRING,
   LLM_RESPONSE STRING,
   CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

cursor.executemany("""
INSERT INTO UCI_CREDIT_DEFAULT_LLM_RESULTS (ROW_NUM, CREDITCUSTID, NAME, PROMPT, LLM_RESPONSE)
VALUES (%s, %s, %s, %s, %s)
""", results)
conn.commit()
store_end = time.time()

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

print("✅ Done. Check UCI_CREDIT_DEFAULT_LLM_RESULTS in Snowflake.")

