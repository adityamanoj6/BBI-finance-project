from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import subprocess

default_args = {
    'owner': 'aditya',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'clean_and_upload_all_files',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['snowflake', 'cleaning', 'etl']
)

HOME = os.path.expanduser('~')
STAGE_FQ = '@FINANCE_PROJECT.ANALYTICS.MY_STAGE'

files_specs = [
    {
        'unclean': 'bank_customer_churn_unclean.csv',
        'cleaned': 'bank_customer_churn_cleaned.csv',
        'type': 'churn',
    },
    {
        'unclean': 'uci_credit_default_unclean.csv',
        'cleaned': 'uci_credit_default_cleaned.csv',
        'type': 'credit_default',
    },
    {
        'unclean': 'personal_loan_modeling_unclean.csv',
        'cleaned': 'personal_loan_modeling_cleaned.csv',
        'type': 'personal_loan',
    },
]

def run_cmd(cmd):
    print(f"Running: {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(res.stdout)
    if res.returncode != 0:
        print("ERROR:", res.stderr)
        res.check_returncode()

def download_and_clean():
    # download each unclean file
    for spec in files_specs:
        unclean = spec['unclean']
        cleaned = spec['cleaned']
        # GET from landing
        run_cmd(f'snowsql -c my_connection -q "GET {STAGE_FQ}/landing/{unclean} file://{HOME}/ OVERWRITE=TRUE;"')

        src = os.path.join(HOME, unclean)
        dst = os.path.join(HOME, cleaned)
        if not os.path.exists(src):
            print(f"Missing downloaded file, skipping: {src}")
            continue

        df = pd.read_csv(src)
        df.columns = df.columns.str.strip().str.lower()

        if spec['type'] == 'churn':
            required = []
            if 'customerid' in df.columns:
                required.append('customerid')
            if 'customer_id' in df.columns:
                required.append('customer_id')
            required += ['creditscore', 'balance']
            df.dropna(subset=required, inplace=True)
            if 'geography' in df.columns:
                df['geography'] = df['geography'].astype(str).str.lower().str.title()
            if 'gender' in df.columns:
                df['gender'] = df['gender'].astype(str).str.lower().str.title()
        elif spec['type'] == 'credit_default':
            df.dropna(subset=['limit_bal', 'age', 'pay_0'], inplace=True)
        elif spec['type'] == 'personal_loan':
            df.dropna(subset=['income', 'ccavg', 'mortgage'], inplace=True)

        df.drop_duplicates(inplace=True)
        df.to_csv(dst, index=False)
        print(f"Cleaned {unclean} -> {cleaned} (shape: {df.shape})")

def upload_cleaned():
    # ensure cleaned folder exists implicitly by putting into it
    for spec in files_specs:
        cleaned = spec['cleaned']
        path = os.path.join(HOME, cleaned)
        if os.path.exists(path):
            run_cmd(f'snowsql -c my_connection -q "PUT file://{path} {STAGE_FQ}/cleaned AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"')
    # list to confirm
    run_cmd(f'snowsql -c my_connection -q "LIST {STAGE_FQ}/cleaned/"')

with dag:
    t1 = PythonOperator(
        task_id='download_and_clean_all',
        python_callable=download_and_clean
    )
    t2 = PythonOperator(
        task_id='upload_all_cleaned',
        python_callable=upload_cleaned
    )

    t1 >> t2
