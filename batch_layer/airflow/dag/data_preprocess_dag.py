from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

SPARK_APPLICATION_FILE_PHASE1 = "spark/data-preprocess.yaml"
SPARK_APPLICATION_FILE_PHASE2 = "spark/feature-engineering.yaml"

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
        application_file=SPARK_APPLICATION_FILE_PHASE1,
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )

    phase2_feature_engineering = SparkKubernetesOperator(
        task_id="phase2_feature_engineering",
        namespace="socialrec",
        application_file=SPARK_APPLICATION_FILE_PHASE2,
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=False,
        do_xcom_push=False,
    )

    phase3_generate_embeddings = KubernetesPodOperator(
        task_id="phase3_generate_embeddings",
        namespace="socialrec",
        image="socialrec-pytorch-embeddings:v1",
        cmds=["python", "/app/generate_embeddings.py"], 
        name="generate-embeddings-pod",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        env_vars={
            "MINIO_ENDPOINT": "http://minio-batch:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "socialrec-batch",
            "ASSETS_ENDPOINT": "http://minio:9000"
        }
    )

    end = EmptyOperator(task_id="end")

    start >> phase1_preprocess >> [phase2_feature_engineering, phase3_generate_embeddings] >> end