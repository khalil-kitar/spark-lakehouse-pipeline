from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_pipeline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    description='Execute Spark pipeline jobs in order on Kubernetes',
    tags=['spark', 'kubernetes'],
) as dag:

    with TaskGroup("bronze") as bronze:
        task_raw_to_bronze = KubernetesPodOperator(
            task_id='ais_raw_to_bronze',
            name='ais-raw-to-bronze',
            namespace='streaming',
            image='khalilkitar/spark-pipeline:3.5.1-with-jobs-v3',
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://kubernetes.default.svc:443",
                "--deploy-mode", "cluster",
                "--name", "ais-raw-to-bronze",
                "--conf", "spark.kubernetes.namespace=streaming",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
                "--conf", "spark.kubernetes.container.image=khalilkitar/spark-pipeline:3.5.1-with-jobs-v3",
                "local:///app/ais_raw_to_bronze.py"
            ],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    with TaskGroup("silver") as silver:
        task_bronze_to_silver = KubernetesPodOperator(
            task_id='ais_bronze_to_silver',
            name='ais-bronze-to-silver',
            namespace='streaming',
            image='khalilkitar/spark-pipeline:3.5.1-with-jobs-v3',
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://kubernetes.default.svc:443",
                "--deploy-mode", "cluster",
                "--name", "ais-bronze-to-silver",
                "--conf", "spark.kubernetes.namespace=streaming",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
                "--conf", "spark.kubernetes.container.image=khalilkitar/spark-pipeline:3.5.1-with-jobs-v3",
                "local:///app/ais_bronze_to_silver.py"
            ],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    with TaskGroup("gold") as gold:
        task_silver_to_gold_dimensions = KubernetesPodOperator(
            task_id='ais_silver_to_gold_dimensions',
            name='ais-silver-to-gold-dimensions',
            namespace='streaming',
            image='khalilkitar/spark-pipeline:3.5.1-with-jobs-v3',
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://kubernetes.default.svc:443",
                "--deploy-mode", "cluster",
                "--name", "ais-silver-to-gold-dimensions",
                "--conf", "spark.kubernetes.namespace=streaming",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
                "--conf", "spark.kubernetes.container.image=khalilkitar/spark-pipeline:3.5.1-with-jobs-v3",
                "local:///app/ais_silver_to_gold_dimensions.py"
            ],
            get_logs=True,
            is_delete_operator_pod=True,
        )

        task_silver_to_gold_fact = KubernetesPodOperator(
            task_id='ais_silver_to_gold_fact',
            name='ais-silver-to-gold-fact',
            namespace='streaming',
            image='khalilkitar/spark-pipeline:3.5.1-with-jobs-v3',
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://kubernetes.default.svc:443",
                "--deploy-mode", "cluster",
                "--name", "ais-silver-to-gold-fact",
                "--conf", "spark.kubernetes.namespace=streaming",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
                "--conf", "spark.kubernetes.container.image=khalilkitar/spark-pipeline:3.5.1-with-jobs-v3",
                "local:///app/ais_silver_to_gold_fact.py"
            ],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    export_parquet = KubernetesPodOperator(
        task_id='delta_to_parquet',
        name='delta-to-parquet',
        namespace='streaming',
        image='khalilkitar/spark-pipeline:3.5.1-with-jobs-v3',
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",
            "--name", "delta-to-parquet",
            "--conf", "spark.kubernetes.namespace=streaming",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa",
            "--conf", "spark.kubernetes.container.image=khalilkitar/spark-pipeline:3.5.1-with-jobs-v3",
            "local:///app/delta_to_parquet.py"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    
    bronze >> silver >> [gold.task_silver_to_gold_dimensions, gold.task_silver_to_gold_fact] >> export_parquet
