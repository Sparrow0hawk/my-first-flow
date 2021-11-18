from datetime import datetime, timedelta
from textwrap import dedent

# the DAG object
from airflow import DAG

# import a bash operator
from airflow.operators.bash import BashOperator

# specify default task parameters
default_args = {
    "owner": "sparrow0hawk",
    "depends_on_past": False,
    "email": ["alexjcoleman@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    # and others at https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/index.html#airflow.models.BaseOperator
}

# now the DAG
with DAG(
    "tutorial",
    default_args=default_args,
    description="My first airflow script",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 17),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = BashOperator(task_id="print_the_date", bash_command="date")

    t2 = BashOperator(
        task_id="sleep", depends_on_past=False, bash_command="sleep 5", retries=3
    )

    templated_cmd = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macro.ds_add(ds, 7) }}"
        echo "{{ params.my_param }}"
    {% endfor %}
        """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_cmd,
        # can pass in dict of parameters to templated cmd
        params={"my_param": "Parameter I passed in"},
    )

    t1.doc_md = dedent(
        """
        # This is some task documentation
        You can document tasks in markdown. Which is *wicked* and _amazing_!.
        ![Michael the good place](https://i.guim.co.uk/img/media/1fbeb305dd8b3856e4409c5e1040f77aaf32125e/329_112_2488_1493/master/2488.jpg?width=1200&height=1200&quality=85&auto=format&fit=crop&s=42fd23bda586b0692c4c031e57c16711)
        """
    )

    dag.doc_md = """
    Documenting the big old DAG
    ## Do you like DAGS?
    """

    # setting up dependencies
    # we can use bitshift operator for this
    # or use t1.set_downstream(t2)
    t1 >> [t2, t3]
