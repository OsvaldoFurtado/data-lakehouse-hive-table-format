import os
import boto3 # type: ignore

from airflow import DAG # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore


DAG_ID = os.path.basename(__file__).replace(".py", "")
DEFAULT_ARGS={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1)
}

# Minio Data
minio_endpoint = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket = "lakehouse"
path_to_delete = "gold/trino_queries/"


#Delete old queried data files
def clear_minio_path():
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=path_to_delete)
    
    for page in pages:
        if "Contents" in page:
            objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects_to_delete})




with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Create Gold Data',
    schedule_interval=None,
    template_searchpath=['/opt/airflow/dags/include/trino/scripts/'],
    tags=["data lake"],
) as dag:

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    clean_old_query_data_task = PythonOperator(
        task_id='clean_old_query_data',
        python_callable=clear_minio_path,
    )

    with open('/opt/airflow/dags/include/trino/scripts/gold_trino_queries.sql') as sql_file:
        sql_list = list(filter(None, sql_file.read().split(';')))

    trino_task = SQLExecuteQueryOperator(
            task_id="run_trino_task",
            sql=sql_list,
            conn_id="trino_default",
            split_statements=True
    )

begin >> clean_old_query_data_task >> trino_task >> end