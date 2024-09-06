import requests
import pandas as pd
import json
from utilities import etl
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
# from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

def extract_movies():
    base_url = 'https://api.themoviedb.org/3/movie/popular'
    endpoint= '?page=2'
    auth_key = Variable.get('the_moviedb_auth_key')
    # auth_key = os.getenv('AUTH_KEY')

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
    # ti.xcom_push("movie_data", results)

    return results


def transform_data(result_data):

    # result_data = ti.xcom_pull(key = "movie_data", task_ids = "task_extract")

    df = pd.DataFrame(result_data)
    
    df['record_loaded_at'] = datetime.now(timezone.utc)
    _df = df[['adult', 'backdrop_path', 'genre_ids', 'id']]
    print(_df)
    # ti.xcom_push("api_df", _df)

    return _df

extract = extract_movies()
transform_data(extract)