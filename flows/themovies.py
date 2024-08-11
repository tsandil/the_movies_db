import os
import requests
import pandas as pd
import json
from utilities import etl
from datetime import datetime, timezone

def extract_movies():
    base_url = 'https://api.themoviedb.org/3/movie/popular'
    endpoint= '?page=2'
    auth_key = os.getenv('AUTH_KEY')

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

    return results


def transform_data(result_data):

    df = pd.DataFrame(result_data)

    df['record_loaded_at'] = datetime.now(timezone.utc)

    print(df)
    return df


def load_dataframe(df):

    schema_name = 'themoviedb'
    db_name = 'my_database1' 
    table_name = 'popular_movies'

    try:
        details = {
            'schema_name':schema_name,
            'db_name':db_name,
            'table_name':table_name
        }
        postgres = etl.PostgresqlDestination(db_name=db_name)

        postgres.write_dataframe(df=df,details=details)

        print(f'Data was loaded successfully....')
    except Exception as e:
        print(f"Data failed to load:\n {e}")
        postgres.close_connection()

if __name__ == '__main__':
    extract_results = extract_movies()
    dataframe= transform_data(extract_results)
    load_dataframe(dataframe)
