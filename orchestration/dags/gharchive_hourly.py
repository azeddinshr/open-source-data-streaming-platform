from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Jinja templates resolved by Airflow for each run's hourly data interval
HOUR = "{{ data_interval_start.strftime('%Y-%m-%dT%H') }}"
DATE = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
HOUR_NUM = "{{ data_interval_start.hour }}"

RAW_DIR = "/data/raw/gharchive"
PARQUET_ROOT = "/data/parquet/gharchive_events"

SPARK_ENV = {"PYSPARK_SUBMIT_ARGS": "--driver-memory 2g pyspark-shell"}
SPARK_JDBC_ENV = {
    "PYSPARK_SUBMIT_ARGS": (
        "--driver-memory 2g "
        "--packages org.postgresql:postgresql:42.7.4 "
        "--conf spark.jars.ivy=/tmp/.ivy2 "
        "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
        "pyspark-shell"
    )
}

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
DBT_ENV = {
    "DBT_PG_HOST": "postgres",
    "DBT_TARGET_PATH": "/tmp/dbt_target",
    "DBT_LOG_PATH": "/tmp/dbt_logs",
}

with DAG(
    dag_id="gharchive_hourly",
    description="GH Archive hour -> Parquet -> Postgres raw -> dbt",
    schedule="@hourly",
    start_date=datetime(2026, 10, 12),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/batch"],
    tags=["batch", "gharchive"],
) as dag:

    # GH Archive publishes an hour shortly after it ends, so a 404 means "try again later"
    download = BashOperator(
        task_id="download_archive_hour",
        bash_command=(
            "cd / && python /opt/airflow/batch/download_gharchive.py "
            f"--start {HOUR} --end {HOUR}"
        ),
        retries=12,
    )

    to_parquet = BashOperator(
        task_id="spark_to_parquet",
        bash_command=(
            "python /opt/airflow/batch/spark_gharchive_to_parquet.py "
            f"--input {RAW_DIR}/{DATE}-{HOUR_NUM}.json.gz"
        ),
        env=SPARK_ENV,
        append_env=True,
    )

    load_staging = BashOperator(
        task_id="spark_load_staging",
        bash_command=(
            "python /opt/airflow/batch/spark_parquet_to_postgres.py "
            f"--input {PARQUET_ROOT}/archive_date={DATE}/archive_hour={HOUR_NUM} "
            f"--base-path {PARQUET_ROOT}"
        ),
        env=SPARK_JDBC_ENV,
        append_env=True,
    )

    merge = SQLExecuteQueryOperator(
        task_id="merge_into_raw",
        conn_id="analytics_pg",
        sql="sql/merge_gharchive.sql",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    download >> to_parquet >> load_staging >> merge >> dbt_build >> dbt_freshness