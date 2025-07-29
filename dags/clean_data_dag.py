from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

LANDING_DIR = os.path.expanduser("~/airflow/data/landing")
CLEANED_DIR = os.path.expanduser("~/airflow/data/cleaned")

def clean_bank_customer_churn():
    df = pd.read_csv(f"{LANDING_DIR}/bank_customer_churn_unclean.csv")
    # Normalize column names: lowercase and strip spaces
    df.columns = df.columns.str.strip().str.lower()

    df.dropna(subset=["customerid", "creditscore", "balance"], inplace=True)
    df['geography'] = df['geography'].str.title()
    df['gender'] = df['gender'].str.title()
    df.drop_duplicates(inplace=True)
    df.to_csv(f"{CLEANED_DIR}/bank_customer_churn_cleaned.csv", index=False)

def clean_uci_credit_default():
    df = pd.read_csv(f"{LANDING_DIR}/uci_credit_default_unclean.csv")
    df.columns = df.columns.str.strip().str.lower()

    df.dropna(subset=["limit_bal", "age", "pay_0"], inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv(f"{CLEANED_DIR}/uci_credit_default_cleaned.csv", index=False)

def clean_personal_loan_modeling():
    df = pd.read_csv(f"{LANDING_DIR}/personal_loan_modeling_unclean.csv")
    df.columns = df.columns.str.strip().str.lower()

    df.dropna(subset=["income", "ccavg", "mortgage"], inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv(f"{CLEANED_DIR}/personal_loan_modeling_cleaned.csv", index=False)

with DAG(
    dag_id="clean_data_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cleaning", "ETL"],
) as dag:

    clean_churn = PythonOperator(
        task_id="clean_bank_customer_churn",
        python_callable=clean_bank_customer_churn
    )

    clean_credit = PythonOperator(
        task_id="clean_uci_credit_default",
        python_callable=clean_uci_credit_default
    )

    clean_loan = PythonOperator(
        task_id="clean_personal_loan_modeling",
        python_callable=clean_personal_loan_modeling
    )

clean_churn >> clean_credit >> clean_loan

