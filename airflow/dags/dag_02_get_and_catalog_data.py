import os

from airflow import DAG # type: ignore
from airflow.providers.ssh.operators.ssh import SSHOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore


DAG_ID = os.path.basename(__file__).replace(".py", "")

MYSQL_USER= os.getenv("MYSQL_USER")
MYSQL_PASSWORD= os.getenv("MYSQL_PASSWORD")
MYSQL_URL= os.getenv("MYSQL_URL")
POSTGRES_USER= os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD= os.getenv("POSTGRES_PASSWORD")
POSTGRES_URL= os.getenv("POSTGRES_URL")

DEFAULT_ARGS={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1)
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Extract data from PostgreSQL and MySQL',
    schedule_interval=None,
    tags=["data lake"]
) as dag:

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")


    copy_to_bronze = SSHOperator(
        task_id='run_spark_job_bronze_test',
        ssh_conn_id='spark_default',  
        cmd_timeout=120,
        command = f"""
            export JAVA_HOME=/opt/bitnami/java
            export SPARK_HOME=/opt/bitnami/spark
            export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
            export MYSQL_USER={MYSQL_USER}
            export MYSQL_PASSWORD={MYSQL_PASSWORD}
            export MYSQL_URL={MYSQL_URL}
            export POSTGRES_USER={POSTGRES_USER}
            export POSTGRES_PASSWORD={POSTGRES_PASSWORD}
            export POSTGRES_URL={POSTGRES_URL}

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
                --name bronze \
                /opt/spark/work-dir/bronze_spark_job.py""",
    )    

begin >> copy_to_bronze >> end