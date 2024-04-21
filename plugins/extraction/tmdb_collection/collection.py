import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import date, datetime
from googlecloud.read_data_gcs import read_blob, list_blobs
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager
from googlecloud.read_data_bigquery import load_data_from_table
from dotenv import load_dotenv
import logging
import requests
from importlib import reload
import concurrent.futures
import json
import shutil


def get_tmdb_collection_id_gcs() -> pd.Series:
    """
    Retrieves the TMDB collection IDs from files stored in Google Cloud Storage (GCS).

    Returns:
        pd.Series: A pandas Series containing the TMDB collection IDs as integers.
    """
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_movie_details")
    interested_col = ['belongs_to_collection']
    df =  pd.DataFrame()
    #read file from gcs and get interested col
    for filename in filenames:
        file_content = read_blob(bucket_name, filename)[interested_col]
        file_content = file_content.dropna()
        df = pd.concat([df, file_content], axis=0)
    return pd.json_normalize(df['belongs_to_collection'])["id"].astype(int)

def chunks(series: pd.Series, length_pieces: int = 20):
    """
    Splits a pandas Series into chunks of specified length.

    Parameters:
        series (pd.Series): The pandas Series to be split into chunks.
        length_pieces (int): The length of each chunk. Default is 20.

    Returns:
        list: A list of pandas Series, where each Series represents a chunk of the original Series.
    """
    indices = []
    counter = 0
    while len(indices) < len(series):
        indices.extend([counter] * length_pieces)
        counter += 1

    indices = np.array(indices)[:len(series)]
    return [series.loc[indices == i] for i in np.unique(indices)]

def collection_info_chunks(chunk:list):
    """
    Retrieves detailed information about movie collections from the TMDB API for one chunk of collection ids.

    Args:
        chunks (list): A list of series of collection IDs.

    Returns:
        dict: A dictionary containing the collection ID as the key and the collection data as the value.
    """
    load_dotenv()
    AUTHORIZATION = os.getenv("Authorization") 
    headers = {
        "accept": "application/json",
        "Authorization": AUTHORIZATION
    }
    logging.info("Start Thread")
    responses = {}
    for collection_id in chunk:
        url = f"https://api.themoviedb.org/3/collection/{collection_id}?language=en-US"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            collection_data = response.json()
            responses[collection_id] = collection_data
        else:
            raise Exception("Unable to retrieve TMDB data")
    return responses

def get_initial_collection_tmdb_details(file_path):
    """
    Retrieves collection details for all collection ids from TMDB API and saves the results to a JSON file.

    Args:
        file_path (str): The file path where the JSON file will be saved.

    Returns:
        None
    """
    reload(logging)
    collection_ids = get_tmdb_collection_id_gcs().drop_duplicates()
    chunks_list = chunks(collection_ids)
    collection_results = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(collection_info_chunks, chunks_list)

    for result in results:
        collection_results.update(result)

    folder_path = file_path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    with open(os.path.join(folder_path, "raw_collection_data.json"), "w") as f:
        json.dump(collection_results, f)
    
    print(os.path.join(folder_path, "raw_collection_data.json"))

def upload_raw_initial_collection_tmdb_details_gcs():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    plugins_dir = os.path.dirname(os.path.dirname(script_dir))
    interested_dir = os.path.join(plugins_dir, "/googlecloud")
    json_path = os.path.join(interested_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    bucket_name = "movies_tmdb"

    try:   
        historicaldata_dir = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
        str_directory = os.path.join(historicaldata_dir, 'raw_historical_data/tmdb_collection')
        directory = Path(str_directory)
        filenames = list([file.name for file in directory.glob('*.json')])
        delete_many_blobs(bucket_name, filenames)
        print(f"{filenames=}")
        upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
    except Exception as e:
        print(f"Error in uploading TMDB raw data to cloud storage \n Error details: {e}")

def get_raw_initial_collection_tmdb_details_gcs():
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_collection_data")
    collection_results = {}
    for filename in filenames:
        file_content = read_blob(bucket_name, filename, json_as_dict=True)
        collection_results.update(file_content)
    return collection_results

def clean_raw_collections_details(save_file_path:str, return_df=False):
    """
    Cleans the raw collection details from json file and saves the cleaned results to a CSV file.

    Args:
        raw_file_path (str): The file path of the raw collection details JSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath (str) or dataframe (pd.Dataframe)
    """

    collection_results = get_raw_initial_collection_tmdb_details_gcs()

    cleaned_results = []
    for key, items in collection_results.items():
        parts_interest =[part for part in items["parts"] if part["media_type"] == "movie" and part["release_date"] != "" and datetime.strptime(part["release_date"], "%Y-%m-%d").year < 2020]
        if len(parts_interest) > 0:
            avg_popularity_before_2020 = sum([part["popularity"] for part in parts_interest]) / len(parts_interest)
        else:
            avg_popularity_before_2020 = None
        cleaned_results.append((items["id"], items["name"].strip(), len(parts_interest), avg_popularity_before_2020))

    cleaned_df = pd.DataFrame(cleaned_results, 
                            columns=["collection_id", "name", "number_movies_before_2020", "avg_popularity_before_2020"])
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        cleaned_df.to_csv(os.path.join(folder_path, "cleaned_collection_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_collection_info.csv")
    else:
        return cleaned_df
    
#### Functions for updating

def collection_ids_to_update() -> pd.Series:
    query_movie = '''
    SELECT DISTINCT CAST(collection_id AS INT64) AS collection_id
    FROM `is3107-418809.movie_dataset.movie`
    WHERE collection_id IS NOT NULL;
    '''
    collection_movie = load_data_from_table(query_movie)
    collection_movie_set = set(collection_movie)
                               
    query_collection = '''
    SELECT collection_id
    FROM `is3107-418809.movie_dataset.collection`
    '''
    now_collection = load_data_from_table(query_collection)
    now_collection_set = set(now_collection)  

    #return those those in movie table that does not exist in collection table
    #The difference() method returns a set that contains the difference between two sets. As a shortcut, you can use the - operator instead.
    #The returned set contains items that exist only in the first set, and not in both sets.
    difference = collection_movie_set - now_collection_set

    return pd.Series(list(difference))


def get_collection_tmdb_details(collection_ids):
    """
    Retrieves collection details for all collection ids from TMDB API, and upload to google cloud storage

    Args:
        file_path (str): The file path where the JSON file will be saved.

    Returns:
        None
    """
    chunks_list = chunks(collection_ids)
    collection_results = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(collection_info_chunks, chunks_list)

    for result in results:
        collection_results.update(result)
    
    #to keep a copy to google cloud storage
    script_dir = os.path.dirname(os.path.realpath(__file__))
    plugins_dir = os.path.dirname(os.path.dirname(script_dir))
    interested_dir = os.path.join(plugins_dir, "/googlecloud")
    json_path = os.path.join(interested_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    file_path = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
    str_directory = os.path.join(file_path, 'update_data/tmdb_collection')
    
    folder_path = str_directory
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    date_now = datetime.now().date()
    with open(os.path.join(folder_path, f"update_raw_collection_data_{date_now}.json"), "w") as f:
        json.dump(collection_results, f)

    try:   
        bucket_name = "update_movies_tmdb"
        directory = Path(folder_path)
        filenames = list([file.name for file in directory.glob('*.json')])
        print(f"{filenames=}")
        upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
        #remove directory after upload
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
    except Exception as e:
        print(f"Error in uploading TMDB raw data to cloud storage \n Error details: {e}")

    return collection_results


def clean_update_collections_details(collection_results:dict, save_file_path:str, return_df=False):
    """
    Cleans the raw collection details python dict and saves the cleaned results to a CSV file.

    Args:
        raw_file_path (str): The file path of the raw collection details JSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath (str) or dataframe (pd.Dataframe)
    """
    cleaned_results = []
    for key, items in collection_results.items():
        parts_interest =[part for part in items["parts"] if part["media_type"] == "movie" and part["release_date"] != "" and datetime.strptime(part["release_date"], "%Y-%m-%d").year < 2020]
        if len(parts_interest) > 0:
            avg_popularity_before_2020 = sum([part["popularity"] for part in parts_interest]) / len(parts_interest)
        else:
            avg_popularity_before_2020 = None
        cleaned_results.append((items["id"], items["name"].strip(), len(parts_interest), avg_popularity_before_2020))

    cleaned_df = pd.DataFrame(cleaned_results, 
                              columns=["collection_id", "name", "number_movies_before_2020", "avg_popularity_before_2020"])
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        cleaned_df.to_csv(os.path.join(folder_path, "cleaned_collection_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_collection_info.csv")
    else:
        return cleaned_df