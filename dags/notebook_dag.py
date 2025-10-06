from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# List of scripts to run
SCRIPTS = {
    "uci_credit_default": "/home/manoj/airflow/scripts/Untitled2.py",
    "personal_loan": "/home/manoj/airflow/scripts/personal_loan.py",
    "bank_churn": "/home/manoj/airflow/scripts/bank_churn.py",
}

def run_script(script_path: str):
    """Execute a Python script as a subprocess."""
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"{script_path} not found")

    result = subprocess.run(
        ["python3", script_path],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Script {script_path} failed:\n\n"
            f"STDOUT:\n{result.stdout}\n\n"
            f"STDERR:\n{result.stderr}"
        )
    print(result.stdout)


# Default args for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="run_scripts_dag",
    default_args=default_args,
    description="Run multiple Python scripts with Airflow",
    schedule=None,   # manual trigger only (Airflow 3.x)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scripts"],
) as dag:

    for task_id, script_path in SCRIPTS.items():
        PythonOperator(
            task_id=f"run_{task_id}",
            python_callable=run_script,
            op_args=[script_path],
        )
