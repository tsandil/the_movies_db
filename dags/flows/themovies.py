import requests
import pandas as pd
import json
from utilities import etl
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import logging

def extract_movies(ti):
    base_url = 'https://api.themoviedb.org/3/movie/popular'
    endpoint= '?page=2'
    auth_key = Variable.get('the_moviedb_auth_key')


    headers = {
        "accept": "application/json",
        "Authorization": auth_key
    }
    url = base_url + endpoint

    response = requests.get(url=url, headers=headers)

    if response.status_code != 200:
        print(f"Error in getting API data: {response.text}")
        print(f"Status Code: {response.status_code}")
    
    _results = json.loads(response.text)

    results = _results['results']
    logger = logging.getLogger(__name__)
    logger.info("This is a log message")


    #Pushing the data from API using XCOMS
    ti.xcom_push("movie_data", results)

    return results


def transform_data(ti):
    logger = logging.getLogger(__name__)
    result_data = ti.xcom_pull(key = "movie_data", task_ids = "task_extract")

    df = pd.DataFrame(result_data)
    
    df['record_loaded_at'] = datetime.now(timezone.utc)
    # _df = df.drop(['genre_ids'], axis=1)
    # df['new_col'] = 22.9

    # Converting genre_ids to json format as it is an array that sqlalchemy engine cant load to postgres using postgres hook due to numpy.ndarray

    df['genre_ids'] = df['genre_ids'].apply(lambda x: json.dumps(x))

    ti.xcom_push("api_df", df)

    logger.info("This is a dataframe", df)
    return df



def load_dataframe(ti):
    
    # postgres_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    # engine = postgres_hook.get_sqlalchemy_engine()

    df = ti.xcom_pull(key = "api_df", task_ids = "task_transform")

    schema_name = 'themoviedb'
    db_name = 'my_database1' 
    table_name = 'popular_movies_test1'
    dest_table = 'popular_movies'
    

    try:
        details = {
            'schema_name':schema_name,
            'db_name':db_name,
            'table_name':table_name,
            'dest_table':dest_table

        }
        postgres = etl.PostgresqlDestination(db_name = db_name)

        postgres.write_dataframe(df = df,details = details)

        print(f'Data was loaded successfully....')
    except Exception as e:
        print(f"Data failed to load:\n {e}")
        postgres.close_connection()




with DAG(
    dag_id='themoviedb_dag',
    start_date=datetime(2023,1,1),
    # default_args=default_args,
    description='A simple DAG implementation',
    tags=['Data_Engineering'],
    schedule=timedelta(minutes=100),
    # catchup=False
):
    task_extract = PythonOperator(task_id = 'task_extract', python_callable = extract_movies, provide_context = True)
    task_transform = PythonOperator(task_id = 'task_transform', python_callable=transform_data, provide_context = True)
    task_load= PythonOperator(task_id = 'task_load', python_callable = load_dataframe, provide_context = True)


task_extract >> task_transform >> task_load





