import os
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="create_required_tables",
    start_date=datetime(2024, 5, 22, 0, 0),
    schedule_interval="@once",
    catchup=False,
    tags=["create_required_tables"],
) as dag:

    start_tasks = DummyOperator(task_id="start_tasks")

    create_country_table = PostgresOperator(
        task_id="create_country_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            CREATE TABLE IF NOT EXISTS country (
                country_id VARCHAR UNIQUE,
                name VARCHAR,
                iso3_code VARCHAR
            );
        """,
    )

    create_gdp_table = PostgresOperator(
        task_id="create_gdp_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            CREATE TABLE IF NOT EXISTS gdp (
                country_id VARCHAR,
                year VARCHAR,
                value VARCHAR,
                CONSTRAINT pk_country_year PRIMARY KEY (country_id, year)
            );
        """,
    )

    create_report_table = PostgresOperator(
        task_id="create_report_table",
        postgres_conn_id=os.environ["DB_CONNECTION"],
        sql="""
            CREATE TABLE IF NOT EXISTS report (
                id VARCHAR UNIQUE,
                name VARCHAR,
                iso3_code VARCHAR,
                "2019" NUMERIC,
                "2020" NUMERIC,
                "2021" NUMERIC,
                "2022" NUMERIC,
                "2023" NUMERIC
            );
        """,
    )

    end_tasks = DummyOperator(task_id="end_tasks")

    (
        start_tasks
        >> [
            create_country_table,
            create_gdp_table,
            create_report_table,
        ]
        >> end_tasks
    )
