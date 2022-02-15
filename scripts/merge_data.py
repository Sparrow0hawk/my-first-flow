from airflow.decorators import task


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
