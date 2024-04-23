import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from googlecloud.read_data_gcs import read_blob, list_blobs
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager
from googlecloud.read_data_bigquery import load_data_from_table
from dotenv import load_dotenv
import logging
import requests
from importlib import reload
import concurrent.futures
import json
from datetime import date

def get_initial_tmdb_people_id_bq() -> pd.Series: 
    """
    Retrieves the people IDs from movie table stored in BigQuery.

    Returns:
        pd.Series: A pandas Series containing the unique TMDB people IDs as integers.
    """
    
    query_people = '''
    SELECT DISTINCT CAST(cast1_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE cast1_id IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT CAST(cast2_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE cast2_id IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT CAST(director_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE director_id IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT CAST(producer_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE producer_id IS NOT NULL
    '''
    
    new_people = load_data_from_table(query_people)
    
    return pd.Series(new_people['people_id'])

def chunks(series: pd.Series, length_pieces: int = 50):
    """
    Splits a pandas Series into chunks of specified length.

    Parameters:
        series (pd.Series): The pandas Series to be split into chunks.
        length_pieces (int): The length of each chunk. Default is 50.

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

def people_info_chunks(chunk:list):
    """
    Retrieves detailed information about people from the TMDB API for one chunk of people ids.

    Args:
        chunks (list): A list of series of people IDs.

    Returns:
        list: A list containing the people data.
    """
    load_dotenv()
    AUTHORIZATION = os.getenv("AUTHORIZATION") 
    headers = {
        "accept": "application/json",
        "Authorization": AUTHORIZATION
    }
    logging.info("Start Thread")
    
    responses = []
    for people_id in chunk:
        url = f"https://api.themoviedb.org/3/person/{people_id}?append_to_response=movie_credits&language=en-US"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data_dict = json.loads(response.text)
            responses.append(data_dict)
        else:
            raise Exception("Unable to retrieve TMDB data")
    return responses
    
def get_initial_people_tmdb_details(file_path):
    """
    Retrieves people details for all people ids from TMDB API and saves the results to a NDJSON file.

    Args:
        file_path (str): The file path where the NDJSON file will be saved.

    Returns:
        None
    """
    reload(logging)
    people_ids = get_initial_tmdb_people_id_bq()
    chunks_list = chunks(people_ids)
    people_results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(people_info_chunks, chunks_list)

    for result in results:
        people_results = people_results + result

    folder_path = file_path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    date_now = date.today().strftime('%Y%m%d')
    filename = f"raw_people_{date_now}.ndjson"

    with open(os.path.join(folder_path, filename), "w") as ndjson_file:
        ndjson_file.write('\n'.join(map(json.dumps, people_results)))
    
    print(os.path.join(folder_path, filename))

def get_raw_tmdb_people_details_gcs():
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_people")
    df =  pd.DataFrame()
    for filename in filenames:
        file_content = read_blob(bucket_name, filename)
        df = pd.concat([df, file_content], axis=0)
    return df

def clean_raw_people_details(save_file_path:str, return_df=False):
    """
    Cleans the raw people details from ndjson file and saves the cleaned results to a CSV file.

    Args:
        raw_file_path (str): The file path of the raw collection details NDJSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath (str) or dataframe (pd.Dataframe)
    """
    
    people_results = get_raw_tmdb_people_details_gcs() 
    
    people_info = pd.DataFrame()
    
    name_lst = []
    bir_lst = []
    gender_lst = []
    pop = []
    cast_credits = []
    crew_credits = []
    known = []
    people_id = []
    
    for index, row in people_results.iterrows():
        # Make the request
        id_ = str(int(row['id']))
        people_id.append(id_)

        try:
            name = row['name']
        except KeyError:
            name = None
        name_lst.append(name)
        try:
            birthday = row['birthday']
        except KeyError:
            birthday = None
        bir_lst.append(birthday)
        try:
            gender = row['gender']
        except KeyError:
            gender = None
        gender_lst.append(gender)
        try:
            tmdb_popularity = row['popularity']
        except KeyError:
            tmdb_popularity = None
        pop.append(tmdb_popularity)
        try:
            known_for = row['known_for_department']
        except KeyError:
            known_for = None
        known.append(known_for)
        
        # Number of credits
        try:
            cast_num = len(row['movie_credits']["cast"])
        except KeyError:
            cast_num = 0
        cast_credits.append(cast_num)
        try:
            crew_num = len(row['movie_credits']["crew"])
        except KeyError:
            crew_num = 0
        crew_credits.append(crew_num)

    people_info['people_id'] = people_id
    people_info['name'] = name_lst
    people_info['birthday'] = bir_lst
    people_info['gender'] = gender_lst
    people_info['tmdb_popularity'] = pop
    people_info['known_for'] = known
    people_info['total_number_cast_credits'] = cast_credits
    people_info['total_number_crew_credits'] = crew_credits
    
    # Convert birthday to Date
    people_info['birthday'] = pd.to_datetime(people_info['birthday']).dt.date
    
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        people_info.to_csv(os.path.join(folder_path, "cleaned_people_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_people_info.csv")
    else:
        return people_info

def new_tmdb_people_id() -> pd.Series:
    """
    Retrieves the people IDs from people table and compare it with latest people IDs from movie table stored in BigQuery.

    Returns:
        pd.Series: A pandas Series containing the unique new TMDB people IDs as integers.
    """
    query_people = '''
    SELECT DISTINCT CAST(people_id as INT64) AS people_id FROM `is3107-418809.movie_dataset.people`
    '''
    
    old_people = load_data_from_table(query_people)
    old_people_set = set(old_people)
    
    query_new_people = '''
    WITH latest_insertion AS (
        SELECT MAX(insertion_datetime) as latest_insertion_datetime
        FROM `is3107-418809.movie_dataset.movie`
    )
    
    SELECT DISTINCT CAST(cast1_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE cast1_id IS NOT NULL AND insertion_datetime = (SELECT latest_insertion_datetime FROM latest_insertion)
    UNION DISTINCT
    SELECT DISTINCT CAST(cast2_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE cast2_id IS NOT NULL AND insertion_datetime = (SELECT latest_insertion_datetime FROM latest_insertion)
    UNION DISTINCT
    SELECT DISTINCT CAST(director_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE director_id IS NOT NULL AND insertion_datetime = (SELECT latest_insertion_datetime FROM latest_insertion)
    UNION DISTINCT
    SELECT DISTINCT CAST(producer_id AS INT64) AS people_id FROM `is3107-418809.movie_dataset.movie`
    WHERE producer_id IS NOT NULL AND insertion_datetime = (SELECT latest_insertion_datetime FROM latest_insertion);
    '''
    
    new_people = load_data_from_table(query_new_people)
    new_people_set = set(new_people)
    
    difference = old_people_set - new_people_set

    return pd.Series(list(difference))

