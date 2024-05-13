"""
Datacoves sample DAG
This DAG is a sample using the Datacoves Airflow Operators
For more info, visit https://docs.datacoves.com/#/reference/airflow/datacoves-operator?id=datacoves-bash-operator
"""

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "John Doe",
        "email": "john@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Datacoves sample DAG",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def datacoves_sample_dag():

    # Calling dbt commands
    dbt_task = DatacovesDbtOperator(
        task_id="run_dbt_task",
        bash_command="dbt debug",
    )

    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    python_task = DatacovesBashOperator(
        task_id="run_python_script",
        bash_command="python orchestrate/python_scripts/sample_script.py",
    )

    # Define task dependencies
    python_task.set_upstream([dbt_task])


# Invoke Dag
dag = datacoves_sample_dag()
dag.doc_md = __doc__
