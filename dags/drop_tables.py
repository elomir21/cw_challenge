import os
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="drop_required_tables",
    start_date=datetime(2024, 5, 22, 0, 0),
    schedule_interval="@once",
    catchup=False,
    tags=["drop_required_tables"],
) as dag:

    start_tasks = DummyOperator(task_id="start_tasks")

    drop_country_table = PostgresOperator(
        task_id="drop_country_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            DROP TABLE country;
        """
    )

    drop_gdp_table = PostgresOperator(
        task_id="drop_gdp_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            DROP TABLE gdp;
        """
    )

    drop_report_table = PostgresOperator(
        task_id="drop_report_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            DROP TABLE report;
        """
    )

    end_tasks = DummyOperator(task_id="end_tasks")

    (
        start_tasks
        >> 
        [
            drop_country_table,
            drop_gdp_table,
            drop_report_table
        ]
        >> end_tasks
    )
