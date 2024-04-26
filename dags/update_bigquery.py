import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from googlecloud.upload_initial_data_bigquery import upload_df_to_table #type:ignore 
from googlecloud.upload_new_data_bigquery import upsert_df_to_table, update_df_to_table #type:ignore
from extraction.tmdb_movie.movie import get_movie_tmdb_details, clean_new_raw_movie_details #type:ignore
from extraction.tmdb_people.people import get_tmdb_people_details, clean_new_raw_people_details, clean_updated_people_details #type:ignore
from extraction.video_stats.clean_per_erd import clean_raw_video_statistics #type:ignore
from extraction.video_stats.collection import extract_raw_video_stats #type:ignore
from extraction.tmdb_collection.collection import collection_ids_to_update, get_collection_tmdb_details, clean_update_collections_details #type:ignore
from extraction.boxoffice_api.boxoffice_func import get_update_batch_dataset, get_update_batch_dataset_by_week #type:ignore
from extraction.boxoffice_api.boxoffice_clean_per_erd import clean_update_weekly_domestic_performance #type:ignore
from datetime import datetime
from dateutil.relativedelta import relativedelta

# at the start of each month, new data is to be ingested into gcs, then transformed and loaded into bigquery

def etl_tmdb_movie_task(**context):
    # initialize start and end dates
    end_date = datetime.strptime(context.get('ds'), "%Y-%m-%d")
    start_date = end_date - relativedelta(weeks=1)

    project_id = "is3107-418809"
    dataset_id = "test_movie_dataset"
    table_id = "movie"
    start_date = (start_date - relativedelta(months=3)).strftime('%Y-%m-%d')
    end_date = (end_date - relativedelta(months=3)).strftime('%Y-%m-%d')
    get_movie_tmdb_details(start_date, end_date)
    df = clean_new_raw_movie_details('', return_df = True)
    upsert_df_to_table(project_id, dataset_id, table_id, ['movie_id'], df, staging_dataset_id="staging_dataset")

def etl_tmdb_person_task(**context):
     # initialize start and end dates
    end_date = datetime.strptime(context.get('ds'), "%Y-%m-%d")
    start_date = end_date - relativedelta(weeks=1)

    project_id = "is3107-418809"
    dataset_id = "test_movie_dataset"
    table_id = "people"
    
    new_people_details, updated_people_details = get_tmdb_people_details(start_date, end_date)

    # New people details in the past week
    new_df = clean_new_raw_people_details(new_people_details, '', return_df = True)
    print(new_df)
    if len(new_df) > 0:
        upsert_df_to_table(project_id, dataset_id, table_id, ['people_id'], new_df, staging_dataset_id="staging_dataset")

    # Updated people details in the past week
    changes_df = clean_updated_people_details(updated_people_details, save_file_path="", return_df=True)
    if len(changes_df) > 0:
        update_df_to_table(project_id, dataset_id, table_id, ['people_id'], changes_df, staging_dataset_id="staging_dataset")

def etl_video_stats_task(**context):
    """
    Extracts, transforms and loads video statistics data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `extract_raw_video_stats` function to extract video stats data based on raw movie details data and upload it to gcs.
    2. Calls the `clean_raw_video_statistics` function to clean the raw video statistics data retrieved from gcs.
    3. Calls the `upsert_df_to_table` function to upsert the cleaned data to the BigQuery collection table.
    """
    # initialize start and end dates
    end_date = datetime.strptime(context.get('ds'), "%Y-%m-%d")
    start_date = end_date - relativedelta(weeks=1)

    # standardize time period with etl_movie_task
    start_date = start_date - relativedelta(months=3)
    end_date = end_date - relativedelta(months=3)

    project_id = "is3107-418809"
    dataset_id = "test_movie_dataset"
    table_id = "video_stats"    
    extract_raw_video_stats(os.path.abspath("./historical_data/update_data/video_stats"), start_date=start_date, end_date=end_date)
    df = clean_raw_video_statistics(save_file_path="", start_date=start_date, end_date=end_date, return_df=True, bucket_name="update_movies_tmdb")
    if len(df) > 0:
        upsert_df_to_table(project_id, dataset_id, table_id, primary_key_columns=["movie_id", "video_key_id"], df=df)

def etl_tmdb_collection_task():
    """
    Extracts, transforms, and loads by appending new collection data into a BigQuery table.
    
    This function performs the following steps:
    1. Calls the `collection_ids_to_update` function which compares the collection ids in movie bigquery table and collection bigquery table, returning a series of collection ids not in collection bigquery database
    2. If there are collection_ids to be appended to existing callection table
        a. Calls the `get_collection_tmdb_details` function to extract raw data from tmdb collection api
        b. Calls the `clean_update_collections_details` function to clean and transform the data and return a DataFrame.
        c. Calls the `upload_df_to_table function` to upload the cleaned DataFrame to the specified BigQuery table, using the "append" mode.
    """
    project_id = "is3107-418809"
    dataset_id = "test_movie_dataset"
    table_id = "collection"
    collection_ids = collection_ids_to_update()
    if len(collection_ids) > 0:
        collection_results = get_collection_tmdb_details(collection_ids)
        update_df = clean_update_collections_details(collection_results, save_file_path='', return_df=True)
        upload_df_to_table(project_id, dataset_id, table_id, update_df, mode="append")  

def etl_weekly_domestic_performance_task(**context): 
    """
    Extracts, transforms, and loads weekly domestic performance data into a BigQuery table.
    
    This function performs the following steps:
    1. Retrieves the start date from the Airflow Variable named "START_DATE" & extracts the year from the start date.
    2. Calls the `get_update_batch_dataset` function to extract raw data from box office mojo (recent 4 weeks of data) and upload to gcs
    3. Calls the `clean_update_weekly_domestic_performance` function to clean the data (all boxofficemojo data - initialisation + update datasets) and return a DataFrame.
    4. Calls the `upload_df_to_table function` to upload the cleaned DataFrame to the specified BigQuery table, using the "truncate" mode.
    """
    # initialize start and end dates
    end_date = datetime.strptime(context.get('ds'), "%Y-%m-%d")
    start_date = end_date - relativedelta(weeks=1)

    project_id = "is3107-418809"
    dataset_id = "test_movie_dataset"
    table_id = "weekly_domestic_performance"
    week = start_date.isocalendar()[1]
    year = start_date.year
    #get_update_batch_dataset(year)
    get_update_batch_dataset_by_week(week, year)
    update_df = clean_update_weekly_domestic_performance(data_path='', return_df=True)
    upload_df_to_table(project_id, dataset_id, table_id, update_df, mode="truncate")


# Airflow DAG
default_args = {
    'start_date': datetime(2024, 4, 1),
    'schedule': None,
    'depends_on_past': False,
    'is_paused_upon_creation': True
}

with DAG(dag_id = 'update_bigquery', default_args=default_args, schedule_interval="0 0 * * 1", catchup=True) as dag:
    etl_tmdb_movie = PythonOperator(task_id='etl_tmdb_movie', python_callable=etl_tmdb_movie_task)
    etl_tmdb_person = PythonOperator(task_id='etl_tmdb_person', python_callable=etl_tmdb_person_task)
    etl_video_stats = PythonOperator(task_id='etl_video_stats', python_callable=etl_video_stats_task)
    etl_tmdb_collection = PythonOperator(task_id='etl_tmdb_collection', python_callable=etl_tmdb_collection_task)
    etl_weekly_domestic_performance = PythonOperator(task_id='etl_weekly_domestic_performance', python_callable=etl_weekly_domestic_performance_task)
 
    etl_tmdb_movie >> [etl_tmdb_person, etl_video_stats, etl_tmdb_collection, etl_weekly_domestic_performance]