from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def get_data(ti):
    my_list = ["sandil", "sunny", "ujjwol", "animesh", "hanxy"]
    ti.xcom_push('data',my_list)
    return ["sandil", "sunny", "ujjwol", "animesh", "hanxy"]

def process_data(ti,item):
    ti.xcom_pull(keys = 'data', task_ids = 'task_a')
    print(f"Processing item {item}")
    time.sleep(5)

with DAG(
    dag_id='expand_dag_2',
    start_date=datetime(2023,1,1),
    description='Testing Parallel Processing',

):
    task_a = PythonOperator(task_id = 'task_a', python_callable= get_data, provide_context = True)
    task_b = PythonOperator(task_id = 'task_b', python_callable= process_data, provide_context = True, max_active_tis_per_dagrun=2)
    
task_a >> task_b