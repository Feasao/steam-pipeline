"""Daily Steam pipeline: collect, upload raw files to S3, then load BigQuery (dbt) and run the
Databricks job in parallel"""
from datetime import datetime, timedelta

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

REPO = "/usr/local/airflow/pipeline"
VENV = "/usr/local/airflow/pipeline_venv/bin"


@dag(
    schedule="0 12 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,  # Steam only returns today's data, so past runs cannot be filled in
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["steam", "elt"],
)
def steam_daily():
    collection = BashOperator(
        task_id="collection",
        bash_command=f"{VENV}/python collection.py",
        cwd=REPO,
        retries=1,
        execution_timeout=timedelta(minutes=90),
    )
    upload_raw = BashOperator(
        task_id="upload_raw",
        bash_command=f"{VENV}/python upload_raw.py",
        cwd=REPO,
    )
    s3_to_bq = BashOperator(
        task_id="s3_to_bq",
        bash_command=f"{VENV}/python s3_to_bq.py",
        cwd=REPO,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{VENV}/dbt deps && {VENV}/dbt build",
        cwd=f"{REPO}/steam",
        env={"DBT_TARGET_PATH": "target_airflow", "DBT_LOG_PATH": "logs_airflow"},
        append_env=True,
    )
    databricks_run = DatabricksRunNowOperator(
        task_id="databricks_run",
        job_name="steam-databricks-daily",
        retries=0, #Retries are handled by the databricks job
        execution_timeout=timedelta(minutes=125),  # Databricks job has 2 hour timeout
    )

    collection >> upload_raw >> [s3_to_bq, databricks_run]
    s3_to_bq >> dbt_build


steam_daily()