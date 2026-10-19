import logging
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

from gustavo_sdk.app.airflow_functions import (
    postgres_to_bucket,
    run_raw,
    run_bronze,
)

logger = logging.getLogger(__name__)


conn_postgres = BaseHook.get_connection("adventureworks_postgres")

host = conn_postgres.host
user = conn_postgres.login
password = conn_postgres.password
port = conn_postgres.port or 5432
database = conn_postgres.schema


conn_bq = BaseHook.get_connection("gcp_default")

service_account_info = json.loads(
    conn_bq.extra_dejson["keyfile_dict"]
)

project_id = service_account_info["project_id"]
system = "adventureworks"
schema = "adventureworks_raw"
bucket_name = "gustavo-data-plataform-raw"


today = datetime.now()

ano = today.year
mes = f"{today.month:02d}"
dia = f"{today.day:02d}"

hive_partition = (
    f"partition_year={ano}/"
    f"partition_month={mes}/"
    f"partition_day={dia}"
)


with DAG(
    dag_id="ingestion_postgres_adventureworks_landing",
    start_date=datetime(2026, 8, 22),
    schedule=None,
    catchup=False,
) as dag:

    with TaskGroup(group_id="customer") as customer:

        customer_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Sales.Customer",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/customer/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        customer_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "customer",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        customer_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "customer",
                "pk": "customerid",
                "credentials_json": service_account_info,
            },
        )

        customer_ingestion >> customer_raw >> customer_bronze



    with TaskGroup(group_id="product") as product:

        product_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Production.Product",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/product/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        product_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "product",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        product_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "product",
                "pk": "productid",
                "credentials_json": service_account_info,
            },
        )

        product_ingestion >> product_raw >> product_bronze

    with TaskGroup(group_id="salesorderheader") as salesorderheader:

        salesorderheader_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Sales.SalesOrderHeader",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/salesorderheader/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        salesorderheader_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "salesorderheader",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        salesorderheader_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "salesorderheader",
                "pk": "salesorderid",
                "credentials_json": service_account_info,
            },
        )

        (
            salesorderheader_ingestion
            >> salesorderheader_raw
            >> salesorderheader_bronze
        )


    with TaskGroup(group_id="salesorderdetail") as salesorderdetail:

        salesorderdetail_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Sales.SalesOrderDetail",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/salesorderdetail/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        salesorderdetail_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "salesorderdetail",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        salesorderdetail_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "salesorderdetail",
                "pk": "salesorderdetailid",
                "credentials_json": service_account_info,
            },
        )

        (
            salesorderdetail_ingestion
            >> salesorderdetail_raw
            >> salesorderdetail_bronze
        )


    with TaskGroup(group_id="purchaseorderheader") as purchaseorderheader:

        purchaseorderheader_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Purchasing.PurchaseOrderHeader",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/purchaseorderheader/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        purchaseorderheader_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "purchaseorderheader",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        purchaseorderheader_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "purchaseorderheader",
                "pk": "purchaseorderid",
                "credentials_json": service_account_info,
            },
        )

        (
            purchaseorderheader_ingestion
            >> purchaseorderheader_raw
            >> purchaseorderheader_bronze
        )


    with TaskGroup(group_id="purchaseorderdetail") as purchaseorderdetail:

        purchaseorderdetail_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Purchasing.PurchaseOrderDetail",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/purchaseorderdetail/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        purchaseorderdetail_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "purchaseorderdetail",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        purchaseorderdetail_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "purchaseorderdetail",
                "pk": "purchaseorderdetailid",
                "credentials_json": service_account_info,
            },
        )

        (
            purchaseorderdetail_ingestion
            >> purchaseorderdetail_raw
            >> purchaseorderdetail_bronze
        )


    with TaskGroup(group_id="vendor") as vendor:

        vendor_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Purchasing.Vendor",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/vendor/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        vendor_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "vendor",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        vendor_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": "adventureworks",
                "table": "vendor",
                "pk": "businessentityid",
                "credentials_json": service_account_info,
            },
        )

        vendor_ingestion >> vendor_raw >> vendor_bronze

    with TaskGroup(group_id="workorder") as workorder:

        workorder_ingestion = PythonOperator(
            task_id="ingestion",
            python_callable=postgres_to_bucket,
            op_kwargs={
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database,
                "table_name": "Production.WorkOrder",
                "bucket_name": bucket_name,
                "bucket_prefix": (
                    f"{system}/workorder/{hive_partition}"
                ),
                "credentials_json": service_account_info,
            },
        )

        workorder_raw = PythonOperator(
            task_id="raw",
            python_callable=run_raw,
            op_kwargs={
                "project_id": project_id,
                "schema": schema,
                "system": system,
                "table": "workorder",
                "bucket_name": bucket_name,
                "year": str(ano),
                "month": mes,
                "day": dia,
                "credentials_json": service_account_info,
            },
        )

        workorder_bronze = PythonOperator(
            task_id="bronze",
            python_callable=run_bronze,
            op_kwargs={
                "project_id": project_id,
                "system": system,
                "table": "workorder",
                "pk": "workorderid",
                "credentials_json": service_account_info,
            },
        )

        workorder_ingestion >> workorder_raw >> workorder_bronze