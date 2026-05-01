from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import psycopg2
import os
import boto3

# Add scripts directory to Python path for DAG imports
sys.path.insert(0, "/opt/airflow/scripts")

from extract import run_extract
from transform import run_pipeline
from s3_utils import upload_to_s3

default_args = {
    "owner": "abinav",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET_NAME = "abinav-dvdrental-bucket1"


def extract_task(**context):
    return run_extract(context["ds"])


def transform_task(**context):
    # run transform
    output = run_pipeline(context["ds"])
    return output


def upload_task(**context):
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="transform")

    # skip if no data
    if not file_path or not os.path.exists(file_path):
        return None

    return upload_to_s3(file_path, context["ds"])


def load_to_postgres(**context):
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="transform")

    # skip if no data
    if not file_path or not os.path.exists(file_path):
        return None

    filename = os.path.basename(file_path)
    s3_key = f"dvd_rentals/date={context['ds']}/{filename}"

    local_download_path = f"/tmp/{filename}"

    s3 = boto3.client("s3")
    s3.download_file(BUCKET_NAME, s3_key, local_download_path)

    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="dvdrental",
        user="postgres",
        password=os.environ.get("DB_PASSWORD")
    )

    cur = conn.cursor()

    with open(local_download_path, "r") as f:
        cur.copy_expert(
            "COPY rentals_warehouse FROM STDIN WITH CSV HEADER DELIMITER ','",
            f
        )

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="dvd_rental_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    t_upload = PythonOperator(
        task_id="upload",
        python_callable=upload_task
    )

    t_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    t_extract >> t_transform >> t_upload >> t_postgres