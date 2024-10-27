from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import time
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': datetime(2021, 1, 1),
}

with DAG(dag_id='dynamic_task_mapping', default_args=default_args, schedule_interval=None) as dag:

    @task
    def get_items():
        return ["sandil", "sunny", "ujjwol", "animesh", "hanxy"]

    @task(max_active_tis_per_dagrun=2)
    def process_item(item):
        print(f"Processing item {item}")
        time.sleep(5)

    items = get_items()
    process_item.expand(item=items)
