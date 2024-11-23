import os
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore


DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1)
}

with DAG(
    dag_id=DAG_ID,
    description="Run all Data Lake DAGs",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=30),
    tags=["data lake"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    trigger_dag_01 = TriggerDagRunOperator(
        task_id="trigger_dag_01",
        trigger_dag_id="dag_01_create_connections",
        wait_for_completion=True,
    )

    trigger_dag_02 = TriggerDagRunOperator(
        task_id="trigger_dag_02",
        trigger_dag_id="dag_02_get_and_catalog_data",
        wait_for_completion=True,
    )

    trigger_dag_03 = TriggerDagRunOperator(
        task_id="trigger_dag_03",
        trigger_dag_id="dag_03_refine_data",
        wait_for_completion=True,
    )

    trigger_dag_04 = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="dag_04_submit_trino_queries",
        wait_for_completion=True,
    )

    trigger_dag_05 = TriggerDagRunOperator(
        task_id="trigger_dag_05",
        trigger_dag_id="dag_05_create_facts_dimensions",
        wait_for_completion=True,
    )


chain(
    begin,
    trigger_dag_01,
    trigger_dag_02,
    trigger_dag_03,
    trigger_dag_04,
    trigger_dag_05,
    end,
)