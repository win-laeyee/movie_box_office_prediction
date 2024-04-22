import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from googlecloud.upload_initial_data_bigquery import upload_df_to_table #type:ignore 
from googlecloud.upload_new_data_bigquery import upsert_df_to_table #type:ignore
from extraction.video_stats.clean_per_erd import clean_raw_video_statistics #type:ignore
from extraction.video_stats.collection import extract_raw_video_stats #type:ignore
from extraction.tmdb_collection.collection import collection_ids_to_update, get_collection_tmdb_details, clean_update_collections_details #type:ignore
from extraction.boxoffice_api.boxoffice_func import get_update_batch_dataset #type:ignore
from extraction.boxoffice_api.boxoffice_clean_per_erd import clean_update_weekly_domestic_performance #type:ignore
from datetime import datetime
from dateutil.relativedelta import relativedelta

# at the start of each month, new data is to be ingested into gcs, then transformed and loaded into bigquery

def set_start_date_task(**context):
    """
    Sets run variables for start date and end date. Downstream tasks should update data with datetimes >= start_date
    and < end_date. END_DATE is defined as the start date of the DAG data interval, START_DATE is the date one month 
    before END_DATE. 

    For example, if DAG starts on 08/04/2024 00:00, START_DATE is 01/04/2024 00:00 and END_DATE is 08/04/2024 00:00.
    To get date variables, use `Variable.get("START_DATE")` or `Variable.get("END_DATE")`.

    Args:
        context (tuple): Kwargs contain DAG run context parameters.
    """
    end_date = datetime.strptime(context.get('ds'), "%Y-%m-%d")
    start_date = end_date - relativedelta(weeks=1)
    Variable.set(key="START_DATE", value=start_date)
    Variable.set(key="END_DATE", value=end_date) 

def etl_tmdb_movie_task():
    # need to name movie blob by date interval (i.e. START_DATE)
    # example: raw_movie_details_{START_DATE}_{END_DATE}.ndjson (raw_movie_details_20240403_20240410)
    # TODO: append + update (tracking changes API)
    pass

def etl_tmdb_person_task():
    # TODO: append + update (tracking changes API)
    pass

def etl_video_stats_task():
    """
    Extracts, transforms and loads video statistics data into a BigQuery table.

    This function performs the following steps:
    1. Calls the `extract_raw_video_stats` function to extract video stats data based on raw movie details data and upload it to gcs.
    2. Calls the `clean_raw_video_statistics` function to clean the raw video statistics data retrieved from gcs.
    3. Calls the `upsert_df_to_table` function to upsert the cleaned data to the BigQuery collection table.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "video_stats"
    start_date, end_date = datetime.strptime(Variable.get("START_DATE"), '%Y-%m-%d %H:%M:%S'), datetime.strptime(Variable.get("END_DATE"), '%Y-%m-%d %H:%M:%S')
    extract_raw_video_stats(os.path.abspath("./raw_data"), start_date=start_date, end_date=end_date)
    df = clean_raw_video_statistics(save_file_path="", start_date=start_date, end_date=end_date, return_df=True)
    upsert_df_to_table(project_id, dataset_id, table_id, primary_key_columns=["movie_id"], df=df)

def etl_tmdb_collection_task():
    """
    Extracts, transforms, and loads by appending new collection data into a BigQuery table. (have yet to test with airflow)
    
    This function performs the following steps:
    1. Calls the `collection_ids_to_update` function which compares the collection ids in movie bigquery table and collection bigquery table, returning a series of collection ids not in collection bigquery database
    2. Calls the `get_collection_tmdb_details` function to extract raw data from tmdb collection api
    3. Calls the `clean_update_collections_details` function to clean and transform the data and return a DataFrame.
    4. Calls the `upload_df_to_table function` to upload the cleaned DataFrame to the specified BigQuery table, using the "append" mode.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "collection"
    collection_ids = collection_ids_to_update()
    collection_results = get_collection_tmdb_details(collection_ids)
    update_df = clean_update_collections_details(collection_results, save_file_path='', return_df=True)
    upload_df_to_table(project_id, dataset_id, table_id, update_df, mode="append")  

def etl_weekly_domestic_performance_task(): 
    """
    Extracts, transforms, and loads weekly domestic performance data into a BigQuery table. (have yet to test with airflow)
    
    This function performs the following steps:
    1. Retrieves the start date from the Airflow Variable named "START_DATE" & extracts the year from the start date.
    2. Calls the `get_update_batch_dataset` function to extract raw data from box office mojo (recent 4 weeks of data) and upload to gcs
    3. Calls the `clean_update_weekly_domestic_performance` function to clean the data (all boxofficemojo data - initialisation + update datasets) and return a DataFrame.
    4. Calls the `upload_df_to_table function` to upload the cleaned DataFrame to the specified BigQuery table, using the "truncate" mode.
    """
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    table_id = "weekly_domestic_performance"
    start_date = datetime.strptime(Variable.get("START_DATE"), '%Y-%m-%d %H:%M:%S')
    year = start_date.year
    get_update_batch_dataset(year)
    update_df = clean_update_weekly_domestic_performance(data_path='', return_df=True)
    upload_df_to_table(project_id, dataset_id, table_id, update_df, mode="truncate")


# Airflow DAG
default_args = {
    'start_date': datetime(2024, 4, 1),
    'schedule': None,
    'depends_on_past': False,
    'is_paused_upon_creation': True
}

with DAG(dag_id = 'update_bigquery', default_args=default_args, schedule_interval='@weekly', catchup=False) as dag:
    set_start_date = PythonOperator(task_id='set_start_date', python_callable=set_start_date_task)
    etl_tmdb_movie = PythonOperator(task_id='etl_tmdb_movie', python_callable=etl_tmdb_movie_task)
    etl_tmdb_person = PythonOperator(task_id='etl_tmdb_person', python_callable=etl_tmdb_person_task)
    etl_video_stats = PythonOperator(task_id='etl_video_stats', python_callable=etl_video_stats_task)
    etl_tmdb_collection = PythonOperator(task_id='etl_tmdb_collection', python_callable=etl_tmdb_collection_task)
    etl_weekly_domestic_performance = PythonOperator(task_id='etl_weekly_domestic_performance', python_callable=etl_weekly_domestic_performance_task)
 
    set_start_date >> etl_tmdb_movie >> [etl_tmdb_person, etl_video_stats, etl_tmdb_collection, etl_weekly_domestic_performance]