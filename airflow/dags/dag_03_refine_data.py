import os

from airflow import DAG # type: ignore
from airflow.providers.ssh.operators.ssh import SSHOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore


DAG_ID = os.path.basename(__file__).replace(".py", "")
DEFAULT_ARGS={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1)
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Refine Bronze Data',
    schedule_interval=None,
    tags=["data lake"]
) as dag:

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    refine_data_task = SSHOperator(
        task_id='refine_data',
        ssh_conn_id='spark_default', 
        cmd_timeout=120, 
        command = f"""
            export JAVA_HOME=/opt/bitnami/java
            export SPARK_HOME=/opt/bitnami/spark
            export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3

            /opt/bitnami/spark/bin/spark-submit \
                --master local[*] \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --jars /opt/spark/work-dir/jars/postgresql-42.2.24.jar,/opt/spark/work-dir/jars/mysql-connector-j-8.2.0.jar,/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,/opt/spark/work-dir/jars/spark-avro_2.13-3.5.3.jar \
                --executor-memory 1g \
                --driver-memory 1g \
                --name silver \
                /opt/spark/work-dir/silver_spark_job.py""",
    )    
        

begin >> refine_data_task >> end