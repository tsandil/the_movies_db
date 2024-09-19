Automated ETL Pipeline for TheMovieDB Using Apache Airflow and PostgreSQL
========

This project implements an ETL(Extract, Transform, Load) pipeline for fetching popular movies from [The Movie Database (TMDB) API](https://www.themoviedb.org/). This pipeline is designed to extract data from API, transform using python code, and loaded into a PostgreSQL Database. This ETL pipeline detects and handles structural schema drift during the Load phase and uses Apache Airflow as an Orchestration tool.

Features
================

This ETL project contains the following features :
- API Integration : Extracting popular movies data from [The Movie Database (TMDB) API](https://developer.themoviedb.org/docs/getting-started)
- Data Transformation : Cleaning and Transforming the data, including Timestamps and JSON formatting for specific columns. And, converting the extracted data into Pandas Dataframes.
- Schema Drift - Detecting Structural Schema Drift in the target PostgreSQL database and handling structural schema drifts by dynamically adding/removing columns and/or managing data-type changes.
- Database Loading : Loads data into a PostgreSQL database, managing schema and merging data into the target table.
- Orchestration : Managed by Apache Airflow for reliable and scalable execution.

Project Goals
================
1. **Data Ingestion** - Create a data ingestion pipeline to extract data from  [The Movie Database (TMDB) API](https://www.themoviedb.org/)
2. **Data Storage** - Create a data storage repository using PostgreSQL.
3. **Data Transformation**  - Create ETL job to extract the data, do simple transformations and load the clean data using Airflow.
4. **Data Pipeline** - Create a data pipeline written in Python that extracts data from API calls and store it in PostgreSQL.
5. **Pipeline Automation** - Create scheduling service using Apace Airflow to trigger the data pipeline and automate the process.


Prerequisites
===========================

1. PostgreSQL Database : Ensure PostgreSQL is installed and running.
2. Airflow : Apache Airflow is used for Orchestration.
    - Install Airflow with [PYPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)

3. Python Libraries : The following libraries has been used
- ```requests```
- ```pandas```
- ```sqlalchemy```
- ```json```
- ```airflow```
- ```PostgresHook``` from ```airflow.providers.postgres```
- **Install Dependencies** : ```pip install -r requirements.txt```
  
  
Configuration
=================================
1. **API Key** : Add your TheMovieDB API key as an Airflow variable named ```the_moviedb_auth_key``` in Airflow's Admin > Variables.

2. **PostgreSQL Connection** : Set up a PostgreSQL connection in Airflow with the connection ID ```themovies_con```.

How to Run
=======
1. Clone the repository and navigate to the project directory.

2. **Start Airflow**  with **Astro CLI** : ```astro dev start```

3. Trigger the DAG : 
- Access the UI at ```http://localhost:8080```
- Trigger the DAG called ```themoviedb_dag```.

4. **Stop Airflow** with **Astro CLI** : ```astro dev stop```

Airflow DAG
=================================
The Airflow DAG consists of three tasks : 
1. **task_extract** : This task calls the TMDB API and fetches all the popular movies data.
2. **task_transform** : Transforms the retrieved data from API, converting to Dataframe using pandas, adding timestamps and formatting the JSON fields.
3. **task_load** : Loads the dataframe into PostgreSQL database while detecting Structural Schema Drift and Handling them.


Connections
=================================
1. **Connection to API**
2. **Connection to PostgreSQL**




File Structure
=================================
- ```dags/flows/themovie.py``` - a python script that extracts data from an API, then transforms using pandas and finally loads to PostgreSQL, orchestrated as tasks using Apache Airflow.
- ```utilities/etl.py```
- ```utilities/queries.py```
- ```.dockerignore``` - 
- ```Dockerfile``` - a text document that contains all the instructions a user could call on the command line to assemble an image.
- ```.gitignore``` - a text document that specifies files that Git should ignore intentionally.
- ```requirements.txt``` - a text document that contains all the libraries required to execute the code. 
- ```setup.py```