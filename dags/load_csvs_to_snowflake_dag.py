from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='upload_and_load_to_snowflake',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['snowflake', 'etl'],
) as dag:

    upload_bank_customer_churn = BashOperator(
        task_id='upload_bank_customer_churn',
        bash_command=(
            "snowsql -c example -q \"PUT file:///home/manoj/airflow/data/cleaned/bank_customer_churn_cleaned.csv "
            "@FINANCE_PROJECT.ANALYTICS.MY_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\""
        ),
    )

    upload_uci_credit_default = BashOperator(
        task_id='upload_uci_credit_default',
        bash_command=(
            "snowsql -c example -q \"PUT file:///home/manoj/airflow/data/cleaned/uci_credit_default_cleaned.csv "
            "@FINANCE_PROJECT.ANALYTICS.MY_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\""
        ),
    )

    upload_personal_loan_modeling = BashOperator(
        task_id='upload_personal_loan_modeling',
        bash_command=(
            "snowsql -c example -q \"PUT file:///home/manoj/airflow/data/cleaned/personal_loan_modeling_cleaned.csv "
            "@FINANCE_PROJECT.ANALYTICS.MY_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\""
        ),
    )

    load_bank_customer_churn = SnowflakeOperator(
        task_id='load_bank_customer_churn',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.BANK_CUSTOMER_CHURN
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/bank_customer_churn_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    load_uci_credit_default = SnowflakeOperator(
        task_id='load_uci_credit_default',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.UCI_CREDIT_DEFAULT
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/uci_credit_default_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    load_personal_loan_modeling = SnowflakeOperator(
        task_id='load_personal_loan_modeling',
        snowflake_conn_id='snowflake_conn',
        sql="""
            COPY INTO FINANCE_PROJECT.ANALYTICS.PERSONAL_LOAN_MODELING
            FROM @FINANCE_PROJECT.ANALYTICS.MY_STAGE/personal_loan_modeling_cleaned.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
            FORCE = TRUE;
        """,
    )

    # Task dependencies
    upload_bank_customer_churn >> load_bank_customer_churn
    upload_uci_credit_default >> load_uci_credit_default
    upload_personal_loan_modeling >> load_personal_loan_modeling

