Automated ETL Pipeline for TheMovieDB Using Apache Airflow and PostgreSQL
========

This project implements an ETL(Extract, Transform, Load) pipeline for fetching popular movies from [The Movie Database (TMDB) API](https://developer.themoviedb.org/reference/movie-popular-list). The pipeline extracts data from the API, transforms it using Python code, and loads it into a PostgreSQL database. Apache Airflow is used as the orchestration tool to manage and automate the entire ETL process.

Features
================

This ETL project contains the following features :
- **API Integration** : Extracting popular movies data from [The Movie Database (TMDB) API](https://developer.themoviedb.org/reference/movie-popular-list)

- **Data Transformation** : Cleaning and Transforming the data, including Timestamps and JSON formatting for specific columns. And, converting the extracted data into Pandas Dataframes.

- **Schema Drift** - Detecting Structural Schema Drift in the target PostgreSQL database and handling structural schema drifts by dynamically adding columns and/or managing data-type changes if required.

- **Dynamic Task Mapping** - Using Apache Airflow's `.expand()` and `.partial()` to create tasks dynamically based on different endpoints, improving the scalability.

- **Parallel Processing** - Optimizing the ETL pipeline by running multiple tasks concurrently, reducing the execution time.

- **Database Loading** : Loads data into a PostgreSQL database, managing schema and merging data into the target table.

- **Orchestration** : Managed by Apache Airflow for reliable and scalable execution.

Project Goals
================
1. **Data Ingestion** - Create a data ingestion pipeline to extract data from  [The Movie Database (TMDB) API](https://developer.themoviedb.org/reference/movie-popular-list)
2. **Data Storage** - Create a data storage repository using PostgreSQL.
3. **Data Transformation**  - Add timestamps and formatting specific fields and use of Pandas Dataframes.
4. **Data Pipeline** - Create a data pipeline written in Python that extracts data from API calls, simple transformation and finally load the dataframe in PostgreSQL.
5. **Pipeline Automation** - Create scheduling service using Apace Airflow to trigger the Data Pipeline and automate the process.


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
   - host: `host.docker.internal` if running from local machine
   - database: `your_database`

How to Run using Astro CLI
=======

To manage and run your Apache Airflow project using Astro CLI, follow these steps:

## 1. Install Astro CLI

Make sure you have Astro CLI installed. You can install it by following the instructions [here](https://docs.astronomer.io/astro/cli/install-cli).

Alternatively, use the command below for installation via `curl`:

```bash
curl -sSL https://install.astronomer.io | sudo bash
```

## 2. Initialize the Project

If you haven't initialized Astro in your project directory, you can do so with:

```bash
astro dev init
```

This will create the necessary Astro project files.

## 3. Start the Astro Project

To start the Airflow environment using Astro, navigate to the project directory and run the following command:

```bash
astro dev start
```

This will build the Docker images, start Airflow, and set up the environment required for your ETL pipeline.

## 4. Access the Airflow Web UI

Once your Astro environment is running, you can access the Airflow web UI at:

```bash
http://localhost:8080
```

Use the default credentials:
- **Username**: `admin`
- **Password**: `admin`

## 5. Stopping the Project

To stop the running Astro project, use:

```bash
astro dev stop
```

This will shut down the Airflow environment cleanly.

## 7. Running Specific DAGs

To trigger a specific DAG run manually, visit the Airflow web UI, navigate to your DAG, and click the **Trigger DAG** button.




File Structure
=================================
- ```dags/flows/themovie.py``` - a python script that extracts data from an API, then transforms using pandas and finally loads to PostgreSQL, orchestrated as tasks using Apache Airflow.
- ```utilities/etl.py``` - a helper python script that ensures data integrity and schema consistency in PostgreSQL.
- ```utilities/queries.py``` - a helper file that has all necessary queries to be executed.
- ```.dockerignore``` - a text documemt that is used to specify files and directories that should be excluded from the Docker image during the build process. 
- ```Dockerfile``` - a text document that contains all the instructions a user could call on the command line to assemble an image.
- ```.gitignore``` - a text document that specifies files that Git should ignore intentionally.
- ```requirements.txt``` - a text document that contains all the libraries required to execute the code. 
- ```setup.py``` - a file configuring the packaging of the utilities module, specifying dependencies like SQLAlchemy and Pandas, and setting up metadata for installation.

## Future Enhancements

This project lays the groundwork for an ETL pipeline fetching popular movie data from The Movie Database (TMDB) API. Here are some potential enhancements that could be implemented in future versions:

1. **Extended Data Sources**: 
   - Integrate additional APIs to enrich the dataset with more information, such as movie ratings, cast and crew details, and user reviews.

2. **Data Analysis and Visualization**: 
   - Create a separate module or dashboard for analyzing the data fetched from TMDB, providing insights into trends, popular genres, and box office performance.

3. **Parallel Processing**:
   - Implement parallel processing techniques to allow multiple tasks to run concurrently, enhancing the efficiency of data extraction and transformation.

4. **Improved Data Transformations**:
   - Integrate **dbt (data build tool)** for efficient data transformations and modeling.
