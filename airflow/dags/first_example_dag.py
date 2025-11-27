# dags/first_example_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "bhawish",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract(**context):
    # simulate extraction
    data = [{"id": 1, "amount": 100}, {"id": 2, "amount": 50}]
    print("EXTRACT ->", data)
    context['ti'].xcom_push(key="raw", value=data)

def clean(**context):
    raw = context['ti'].xcom_pull(key="raw", task_ids="extract")
    cleaned = [ {**r, "amount": float(r["amount"])} for r in raw if r["amount"] >= 0 ]
    print("CLEAN ->", cleaned)
    context['ti'].xcom_push(key="cleaned", value=cleaned)

def aggregate(**context):
    cleaned = context['ti'].xcom_pull(key="cleaned", task_ids="clean")
    total = sum(r["amount"] for r in cleaned)
    print("AGGREGATE -> total=", total)
    context['ti'].xcom_push(key="total", value=total)

def save(**context):
    total = context['ti'].xcom_pull(key="total", task_ids="aggregate")
    # Replace with DB write in real projects. Here we just print.
    print("SAVE -> writing total to storage (simulated):", total)

with DAG(
    dag_id="first_example_dag",
    default_args=default_args,
    description="First example DAG: Extract→Clean→Aggregate→Save",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=["example", "banking"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_clean   = PythonOperator(task_id="clean", python_callable=clean)
    t_agg     = PythonOperator(task_id="aggregate", python_callable=aggregate)
    t_save    = PythonOperator(task_id="save", python_callable=save)

    t_extract >> t_clean >> t_agg >> t_save
