import os
from sqlalchemy.orm import Session # type: ignore

from airflow import DAG
from airflow.models import Connection # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.session import provide_session # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore


DAG_ID = os.path.basename(__file__).replace(".py", "")

# Functions to create connections on Airflow
@provide_session
def create_connection(conn_id, conn_type, host, schema=None, login=None, password=None, port=None, session: Session = None):
    # Checks if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if not existing_conn:
        # Create a new connection
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port,
        )
        session.add(new_conn)
        session.commit()
        print(f"Connection {conn_id} created successfully!")
    else:
        print(f"Connection {conn_id} already exists")


# DAGs configs
DEFAULT_ARGS={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1)
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["data lake"]
) as dag:

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    # Task to create connection for Postgres
    create_postgres_connection = PythonOperator(
        task_id='create_postgres_connection',
        python_callable=create_connection,
        op_kwargs={
            'conn_id': 'postgres_source',
            'conn_type': 'postgres',
            'host': 'postgres',
            'schema': 'sales',
            'login': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'port': 5432,
        }
    )

    # Task to create connection for MySQL
    create_mysql_connection = PythonOperator(
        task_id='create_mysql_connection',
        python_callable=create_connection,
        op_kwargs={
            'conn_id': 'mysql_source',
            'conn_type': 'mysql',
            'host': 'mysql',
            'schema': 'production',
            'login': os.getenv('MYSQL_USER'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'port': 3306,
        }
    )

    # Task to create SSH connnection for Spark
    create_spark_ssh_connection = PythonOperator(
        task_id='create_spark_ssh_connection',
        python_callable=create_connection,
        op_kwargs={
            'conn_id': 'spark_default',
            'conn_type': 'ssh',
            'host': 'spark',
            'login': 'me',
            'password': 'changeme',
            'port': 22,
        }
    )


    # Task to create Trino connnection
    create_trino_connection = PythonOperator(
        task_id='create_trino_connection',
        python_callable=create_connection,
        op_kwargs={
            'conn_id': 'trino_default',
            'conn_type': 'trino',
            'host': 'trino',
            'login': 'admin',
            'password': '',
            'port': 8080,
        }
    )



    # Define a ordem de execução das tarefas
begin >> create_postgres_connection >> create_mysql_connection >> create_spark_ssh_connection >> create_trino_connection >> end
