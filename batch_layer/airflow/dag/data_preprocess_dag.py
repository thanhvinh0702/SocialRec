from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.operators.empty import EmptyOperator

SPARK_APPLICATION_FILE = "spark/data-preprocess.yaml"


with DAG(
    dag_id="data_preprocess",
    description="Sequential Spark preprocessing pipeline for SocialRec batch data.",
    start_date=datetime(2026, 5, 17),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["socialrec", "spark", "preprocess"],
) as dag:
    start = EmptyOperator(task_id="start")

    phase1_preprocess = SparkKubernetesOperator(
        task_id="phase1_preprocess",
        namespace="socialrec",
        application_file=SPARK_APPLICATION_FILE,
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )

    end = EmptyOperator(task_id="end")

    start >> phase1_preprocess >> end
