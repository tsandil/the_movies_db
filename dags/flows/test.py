import requests, json
import pandas as pd


def extract():
    base_url = "https://api.themoviedb.org/3/movie/"
    ep = 'popular'

    endpoint = f'{ep}?'

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZWM1YjZiNmZkNWRmNjkyYWMzOTcxMWIwMWFkMGMwNSIsIm5iZiI6MTcyNjczNjMwMy4wOTAwMywic3ViIjoiNjZhZTI0MTM5OTdmNmE2OTMxZjkyN2NlIiwic2NvcGVzIjpbImFwaV9yZWFkIl0sInZlcnNpb24iOjF9.gOYolIz1ZX1Jc9FrHRtaC0MtTnbiQ6itcWLOcu3cXRQ"
    }

    url = base_url + endpoint

    results = []
    while True:

        print(f"The URL is {url}")

        response = requests.get(url, headers=headers)
        print(f"The response inside loop is {response.status_code}")

        if response.status_code == 400 and json.loads(response.text)["success"] is False:
                break

        if response.status_code != 200:
            raise Exception(f"The status code is {response.status_code} and message is {response.text} and {type(response.text)}")

        result = response.json()
        results += result["results"]
        page_number = result["page"] + 100
        url = f"{base_url}{endpoint}page={page_number}"

    return results

def transform(results: list):

    df = pd.DataFrame(results)

    print(df)



if __name__ == '__main__':
    results = extract()
    transform(results=results)

