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
