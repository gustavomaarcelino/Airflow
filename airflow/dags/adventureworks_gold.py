
import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from gustavo_sdk.app.airflow_functions import run_gold


conn_bq = BaseHook.get_connection("gcp_default")

service_account_info = json.loads(
    conn_bq.extra_dejson["keyfile_dict"]
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SQL_PATH = (
    PROJECT_ROOT
    / "airflow"
    / "plugins"
    / "gustavo_sdk"
    / "sql"
    / "gold"
)


with DAG(
    dag_id="adventureworks_gold",
    start_date=datetime(2026, 8, 22),
    schedule=None,
    catchup=False,
) as dag:

    obt = PythonOperator(
        task_id="obt",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(SQL_PATH / "obt.sql"),
            "credentials_json": service_account_info,
        },
    )