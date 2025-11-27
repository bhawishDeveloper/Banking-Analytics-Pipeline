# from datetime import datetime, timedelta
# from airflow.utils.dates import days_ago

# from airflow import DAG

# # from airflow.operators.bash import BashOperator
# from airflow.providers.standard.operators.bash import BashOperator

# default_args = {
# 	'owner' : 'admin1',
# }

# dag = DAG(
#     dag_id = 'hello_world',
#     description = 'Our first "Hello World" DAG!',
#     default_args = default_args,
#     start_date = days_ago(1),
#     schedule_interval = '@daily',
#     tags = ['beginner', 'bash', 'hello world']
# )

# task = BashOperator(
#     task_id = 'hello_world_task',
#     bash_command = 'echo Hello world once again!',
#     dag = dag
# )

# task
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="my_bash_dag",
    start_date=datetime(2025, 11, 20),
    schedule='@daily',
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Airflow!'"
    )
t1