import json
import logging

import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from utilities import etl


def extract_movies(endpoint, **kwargs):
    ti = kwargs['ti']
    base_url = 'https://api.themoviedb.org/3/movie/'
    auth_key = Variable.get('the_moviedb_auth_key')

    headers = {
        "accept": "application/json",
        "Authorization": auth_key
    }
    url = base_url + endpoint

    results = []

    while True:
        logging.info(f"The URL is : \n{url}")
        print(f"The URL is : \n{url}")
        response = requests.get(url=url, headers=headers)

        if response.status_code == 400 and json.loads(response.text)["success"] is False:
            break
        
        if response.status_code != 200:
            raise Exception(f"The status code is {response.status_code} and message is {response.text} and {type(response.text)}")

        _results = response.json()
        results += _results["results"]
        page_num = _results["page"] + 10
        url = f"{base_url}{endpoint}?page={page_num}"

    #Pushing the data from API using XCOMS
    ti.xcom_push(key = f"{endpoint}_movie_data", value = results)
    logging.info(f"Data successfully pushed to XCom with key: {endpoint}_movie_data")

    return results


def transform_data(endpoint, **kwargs):
    ti = kwargs['ti']

    result_data = ti.xcom_pull(
        key = f"{endpoint}_movie_data",
        task_ids = "task_extract"
        )
    
    if not result_data:
        raise ValueError(f"No data pulled from XCom for endpoint: {endpoint}")

    # Unpacking the data pulled from extract task as it passes LazySequenceSelector[1].
    unpacked_list = [*result_data]
    for data_ in unpacked_list:
        print(f"The data after unpacking is : \n{data_}")
        print(f"The type of unpacked data is : {type(data_)}")
    
    df = pd.DataFrame(data_)

    # Converting genre_ids to json format as it is an array that sqlalchemy engine cant load to postgres using postgres hook due to numpy.ndarray
    df['genre_ids'] = df['genre_ids'].apply(lambda x: json.dumps(x))


    ti.xcom_push(key = f"{endpoint}_api_df", value = df.to_dict(orient='records'))
    logging.info(f"Data successfully pushed to XCom with key: {endpoint}_api_df")


def load_dataframe(endpoint, **kwargs):
    ti = kwargs['ti']

    df_dict = ti.xcom_pull(key = f"{endpoint}_api_df", task_ids = f"task_transform")

    if not df_dict:
        raise ValueError(f"No data pulled from XCom for endpoint: {endpoint}")

    # Unpacking the data from task transform as it passes LazySequenceSelector[1]
    unpacked_list = [*df_dict]
    for data in unpacked_list:
        print(f"The data after unpacking is : \n {data}")
        print(f"The type of unpacked data is : \n {type(data)}")

    # Converting the unpacked data from task transform back to pandas dataframe for loading.
    dataframe = pd.DataFrame(data)
  
    schema_name = 'themoviedb'
    db_name = 'raw_movie_data' 
    table_name = f"{endpoint}_movies"
    primary_key = 'id'

    try:
        details = {
            'schema_name': schema_name,
            'db_name': db_name,
            'table_name': table_name,
            'primary_key': primary_key
        }

        postgres = etl.PostgresqlDestination()

        postgres.merge_tables(df = dataframe, details = details) 

    except Exception as e:
        raise Exception(e)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

with DAG(
    dag_id='themoviedb_dag',
    start_date=datetime(2023,1,1),
    default_args=default_args,
    description='A simple DAG implementation to extract and load themovies_db data',
    tags=["Data_Engineering", "ETL", "Movies ETL"],
    schedule='@daily'
) as dag:

    endpoints = ['popular', 'top_rated', 'now_playing', 'upcoming']

    extract_task = PythonOperator.partial(
        task_id="task_extract",
        python_callable=extract_movies,
    ).expand(op_kwargs=[{"endpoint": endpoint} for endpoint in endpoints])

    transform_task = PythonOperator.partial(
        task_id="task_transform",
        python_callable=transform_data,
    ).expand(op_kwargs=[{"endpoint": endpoint} for endpoint in endpoints])

    load_task = PythonOperator.partial(
        task_id="task_load",
        python_callable=load_dataframe,
    ).expand(op_kwargs=[{"endpoint": endpoint} for endpoint in endpoints])

    extract_task  >> transform_task >> load_task






