
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

    fato_vendas = PythonOperator(
        task_id="fato_vendas",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_vendas.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    fato_compras = PythonOperator(
        task_id="fato_compras",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_compras.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    fato_producao = PythonOperator(
        task_id="fato_producao",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "fato_producao.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    dim_cliente = PythonOperator(
        task_id="dim_cliente",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "dim_cliente.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    dim_produto = PythonOperator(
        task_id="dim_produto",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "dim_produto.sql"
            ),
            "credentials_json": service_account_info,
        },
    )

    dim_fornecedor = PythonOperator(
        task_id="dim_fornecedor",
        python_callable=run_gold,
        op_kwargs={
            "sql_path": str(
                SQL_PATH / "dim_fornecedor.sql"
            ),
            "credentials_json": service_account_info,
        },
    )