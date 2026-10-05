"""Optional Airflow 2.10 DAG scaffold; not part of verified local execution."""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="portfolio_retail_events",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def retail_events():
    @task
    def ingest_source():
        from pipeline.ingest import ingest, export_canonical

        root = os.environ.get("PIPELINE_ROOT", "/opt/airflow/artifacts")
        report = ingest(os.environ["PIPELINE_SOURCE"], root)
        export_canonical(root)
        return report["batch_hash"]

    @task
    def mirror(batch_hash):
        from pipeline.publish_s3 import publish

        return publish(
            os.environ.get("PIPELINE_ROOT", "/opt/airflow/artifacts"),
            batch_hash,
            os.environ["AWS_S3_BUCKET"],
        )

    mirror(ingest_source())


retail_events()
