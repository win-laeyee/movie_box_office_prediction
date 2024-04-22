from airflow import DAG #type: ignore
from airflow.operators.python import PythonOperator #type:ignore
from googlecloud.create_table_bigquery import delete_all_tables, create_dataset_if_not_exists, create_all_tables #type:ignore
from googlecloud.upload_initial_data_bigquery import upload_csv_to_table, upload_df_to_table #type:ignore
from extraction.video_stats.clean_per_erd import clean_raw_video_statistics #type:ignore
from extraction.tmdb_collection.collection import clean_raw_collections_details #type:ignore
from extraction.tmdb_movie.movie import clean_raw_movie_details #type:ignore
from extraction.tmdb_people.people import clean_raw_people_details #type:ignore
from extraction.boxoffice_api.boxoffice_clean_per_erd import get_clean_weekly_domestic_performance #type:ignore
from datetime import datetime, timedelta

# raw data already uploaded to gcs (airflow is used to pull the raw data, transform and load to bigquery)

def setup_bigquery_task():
    """
    Sets up / Initialise BigQuery by deleting existing tables, creating a dataset if it doesn't exist,
    and creating all required tables.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    delete_all_tables(project_id, dataset_id)
    create_dataset_if_not_exists(project_id, dataset_id)
    create_all_tables(project_id, dataset_id)

def etl_tmdb_movie_task():
    """
    Extracts, transforms, and loads TMDB movie data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `clean_raw_movie_details` function to clean and transform the raw data retrieved from gcs.
    2. Calls the `upload_df_to_table` function to upload the cleaned data to the BigQuery movie table.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "movie"
    df = clean_raw_movie_details('', return_df = True)
    upload_df_to_table(project_id, dataset_id, table_id, df, mode="truncate")

def etl_tmdb_person_task():
    """
    Extracts, transforms, and loads TMDB people data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `clean_raw_people_details` function to clean and transform the raw data retrieved from gcs.
    2. Calls the `upload_df_to_table` function to upload the cleaned data to the BigQuery movie table.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "people"
    df = clean_raw_people_details('', return_df = True)
    upload_df_to_table(project_id, dataset_id, table_id, df, mode="truncate")

def etl_video_stats_task():
    """
    Extracts, transforms and loads video statistics data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `clean_raw_video_statistics` function to clean the raw video statistics data retrieved from gcs.
    2. Calls the `upload_df_to_table` function to upload the cleaned data to the BigQuery collection table.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "video_stats"
    df = clean_raw_video_statistics('', return_df = True)
    upload_df_to_table(project_id, dataset_id, table_id, df, mode="truncate")

def etl_tmdb_collection_task():
    """
    Extracts, transforms, and loads TMDB collection data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `clean_raw_collections_details` function to clean and transform the raw data retrieved from gcs.
    2. Calls the `upload_csv_to_table` function to upload the cleaned data to the BigQuery collection table.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "collection"
    df = clean_raw_collections_details('', return_df = True)
    upload_df_to_table(project_id, dataset_id, table_id, df, mode="truncate")

def etl_weekly_domestic_performance_task():
    """
    Extracts, transforms, and loads boxofficemojo weekly domestic performance data into a BigQuery table.
    
    This function performs the following steps:
    1. Calls the `get_clean_weekly_domestic_performance` function to clean and transform the raw data retrieved from gcs.
    2. Calls the `upload_csv_to_table` function to upload the cleaned data to the BigQuery collection table.
    """
    
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "weekly_domestic_performance"
    df = get_clean_weekly_domestic_performance('', return_df = True)
    upload_df_to_table(project_id, dataset_id, table_id, df, mode="truncate")


# Airflow DAG
default_args = {
    'start_date': datetime(2024, 4, 1),
    'schedule': None,
    'depends_on_past': False,
    'is_paused_upon_creation': True
}

with DAG(dag_id = 'initialise_bigquery', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    setup_bigquery = PythonOperator(task_id='setup_bigquery', python_callable=setup_bigquery_task)
    etl_tmdb_movie = PythonOperator(task_id='etl_tmdb_movie', python_callable=etl_tmdb_movie_task)
    etl_tmdb_person = PythonOperator(task_id='etl_tmdb_person', python_callable=etl_tmdb_person_task)
    etl_video_stats = PythonOperator(task_id='etl_video_stats', python_callable=etl_video_stats_task)
    etl_tmdb_collection = PythonOperator(task_id='etl_tmdb_collection', python_callable=etl_tmdb_collection_task)
    etl_weekly_domestic_performance = PythonOperator(task_id='etl_weekly_domestic_performance', python_callable=etl_weekly_domestic_performance_task)

    setup_bigquery >> [etl_tmdb_movie, etl_tmdb_person, etl_video_stats, etl_tmdb_collection] >> etl_weekly_domestic_performance