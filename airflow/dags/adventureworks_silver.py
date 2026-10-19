import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from gustavo_sdk.app.airflow_functions import run_silver


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
    / "pipelines"
)


with DAG(
    dag_id="adventureworks_silver",
    start_date=datetime(2026, 8, 22),
    schedule=None,
    catchup=False,
) as dag:

    fato_vendas = PythonOperator(
        task_id="fato_vendas",
        python_callable=run_silver,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_vendas.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    fato_compras = PythonOperator(
        task_id="fato_compras",
        python_callable=run_silver,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_compras.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    fato_producao = PythonOperator(
        task_id="fato_producao",
        python_callable=run_silver,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_producao.sql"
            ),
            "credentials_json": service_account_info,
        },
    )