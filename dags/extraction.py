from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from hooks.extract import DataExtract


with DAG(
    dag_id="extract_data",
    start_date=datetime(2024, 5, 22, 0, 0),
    schedule_interval="@once",
    catchup=False,
    tags=["extract_data"],
) as dag:

    start_tasks = DummyOperator(task_id="start_tasks")

    test_dag_python = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=DataExtract.get_api_data
    )

    end_tasks = DummyOperator(task_id="end_tasks")

    start_tasks >> test_dag_python >> end_tasks
