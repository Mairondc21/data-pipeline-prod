import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SCRIPT = "/usr/local/airflow/include/bronze_customers_2.py"

def run_spark():
    os.system(f"python {SPARK_SCRIPT}")

default_args = {
    "owner": "Arthur",
    "start_date": datetime(2025,1,1),
    "retries": 1
}

with DAG(
    dag_id="amazon_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["etl"]
) as dag:

    executar_spark = PythonOperator(
        task_id="spark_etl",
        python_callable=run_spark
    )

    executar_spark