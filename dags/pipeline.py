from datetime import timedelta
import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.dag import ScheduleInterval
from airflow.operators.bash import BashOperator
from requests.api import request
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.sql import SQLCheckOperator
from airflow.models.connection import Connection
from airflow import settings


@dag(
    schedule_interval="0 0 * * *",
    start_date=datetime.today() - timedelta(days=2),
    dagrun_timeout=timedelta(minutes=60),
)
def etl_pipeline():
    def branch_func(ti):
        return

    @task
    def create_connection():

        session = settings.Session()  # get the session

        session_check = (
            session.query(Connection).filter_by(conn_id="postgres_default") is not None
        )

        if not session_check:

            c = Connection(
                conn_id="postgres_default",
                conn_type="postgres",
                host="postgres",
                login="airflow",
                password="airflow",
            )

            session.add(c)
            session.commit()

        else:
            logging.info("Connection already exists")
            return 0

    setup_database = PostgresOperator(
        task_id="setup_database",
        postgres_conn_id="postgres_default",
        sql="sql/create_employee_tbl.sql",
    )

    @task
    def get_data():
        import requests

        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"

        data_path = "/tmp/employees.csv"

        with requests.get(url, stream=True) as req:
            req.raise_for_status()
            with open(data_path, "wb") as file:
                for chunk in req.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY \"Employees_temp\" FROM stdin WITH CSV HEADER DELIMITER AS ','",
                file,
            )
        conn.commit()

    check_employees = SQLCheckOperator(
        task_id="Check_employees_tbl",
        conn_id="postgres_default",
        sql='SELECT count(*) FROM public."Employees"',
    )

    check_employees_temp = SQLCheckOperator(
        task_id="Check_employees_temp_tbl",
        conn_id="postgres_default",
        sql='SELECT count(*) FROM public."Employees_temp"',
    )

    @task
    def merge_data():
        query = """
                delete
                from "Employees" e using "Employees_temp" et
                where e."Serial Number" = et."Serial Number";

                insert into "Employees"
                select *
                from "Employees_temp";
                """

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

    (
        create_connection()
        >> setup_database
        >> get_data()
        >> check_employees_temp
        >> merge_data()
        >> check_employees
    )


dag = etl_pipeline()
