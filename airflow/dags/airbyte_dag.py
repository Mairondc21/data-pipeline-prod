from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime

with DAG(
    dag_id="airbyte_sync_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",      # conexão configurada no Airflow
        connection_id="445f6436-d16a-48c6-812e-7f711f0d348f",  # ID da conexão no Airbyte
        asynchronous=True,                        # não bloqueia a task
    )

    wait_sync = AirbyteJobSensor(
        task_id="wait_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id=trigger_sync.output,
    )

    trigger_sync >> wait_sync
