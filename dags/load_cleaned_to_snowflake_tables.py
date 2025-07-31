from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'aditya',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='load_cleaned_to_snowflake_tables',
    default_args=default_args,
    schedule='@daily',  # or change to None for only manual trigger
    catchup=False,
    tags=['snowflake', 'load']
) as dag:

    # 1. Pre-check that cleaned files exist
    check_cleaned_files = BashOperator(
        task_id='check_cleaned_files_exist',
        bash_command="""
        echo "Verifying cleaned files in stage..."
        snowsql -c my_connection -q "LIST @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/bank_customer_churn_cleaned.csv" || (echo "missing bank_customer_churn_cleaned.csv" && exit 1)
        snowsql -c my_connection -q "LIST @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/uci_credit_default_cleaned.csv" || (echo "missing uci_credit_default_cleaned.csv" && exit 1)
        snowsql -c my_connection -q "LIST @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/personal_loan_modeling_cleaned.csv" || (echo "missing personal_loan_modeling_cleaned.csv" && exit 1)
        echo "All required cleaned files are present."
        """
    )

    # 2. Load each cleaned CSV into its Snowflake table
    load_bank_customer_churn = SnowflakeOperator(
        task_id='load_bank_customer_churn',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.BANK_CUSTOMER_CHURN
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/bank_customer_churn_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    load_uci_credit_default = SnowflakeOperator(
        task_id='load_uci_credit_default',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.UCI_CREDIT_DEFAULT
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/uci_credit_default_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    load_personal_loan_modeling = SnowflakeOperator(
        task_id='load_personal_loan_modeling',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.PERSONAL_LOAN_MODELING
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/cleaned/personal_loan_modeling_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    # Dependencies
    check_cleaned_files >> [load_bank_customer_churn, load_uci_credit_default, load_personal_loan_modeling]
