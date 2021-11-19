from datetime import timedelta
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.dag import ScheduleInterval
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    schedule_interval="0 0 * * *",
    start_date=datetime.today() - timedelta(days=2),
    dagrun_timeout=timedelta(minutes=60),
)
def etl_pipeline():
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
        task_id="create_employees",
        postgres_conn_id="postgres_default",
        sql="sql/create_employee_tbl.sql",
    )

    import requests

    @task
    def get_data():
        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"

        response = requests.request("GET", url)

        with open("/usr/local/airflow/dags/files/employees.csv", "w") as file:
            for row in response.text.split("\n"):
                file.write(row)

        postgres_hook = PostgresHook(postgres_conn_id="LOCAL")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        with open("/usr/local/airflow/dags/files/employees.csv", "r") as file:
            cur.copy_from(
                f,
                "Employees_temp",
                columns=[
                    "Serial Number",
                    "Company Name",
                    "Employee Markme",
                    "Description",
                    "Leave",
                ],
                sep=",",
            )
        conn.commit()

    @task
    def merge_data():
        query = """
                delete
                from "Employees" e using Employees_temp et
                where e."Serial Number" = et."Serial Number";

                insert into "Employees"
                select *
                from "Employees_temp";
                """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="LOCAL")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()

            return 0
        except Exception as e:
            return 1

    setup_database >> get_data() >> merge_data()


dag = etl_pipeline()
