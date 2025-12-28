from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import datetime, timedelta

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json
)

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "roopesh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "kalathuru181@gmail.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"


with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 15 * * *",
    catchup=False,
) as dag_produce:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db"
    )
playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="Load JSON data into staging and core tables",
    schedule=None,
    catchup=False,
) as dag_update:

#     wait_for_json = ExternalTaskSensor(
#     task_id="wait_for_produce_json",
#     external_dag_id="produce_json",
#     external_task_id="save_json",
#     execution_delta=timedelta(hours=1),
#     mode="reschedule",
#     timeout=3600,
#     poke_interval=60,
# )


    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Dependency chain
    update_staging >> update_core >> trigger_data_quality


# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule=None,
    catchup=False,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)
    
# Dependency chain
soda_validate_staging >> soda_validate_core  