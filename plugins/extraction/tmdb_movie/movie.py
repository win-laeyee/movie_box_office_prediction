import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import date, datetime
from googlecloud.read_data_gcs import read_blob, list_blobs
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager, upload_blob
from googlecloud.read_data_bigquery import load_data_from_table
from dotenv import load_dotenv
import logging
import requests
from importlib import reload
import concurrent.futures
import json
import shutil

def get_tmdb_movie_id(start_release_date, end_release_date) -> pd.Series:
    
    load_dotenv()
    AUTHORIZATION = os.getenv("AUTHORIZATION") 
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AUTHORIZATION}"
    }
    
    print(AUTHORIZATION)
    url = 'https://api.themoviedb.org/3/discover/movie?language=en-US&page=1&primary_release_date.gte=' \
    + start_release_date + '&primary_release_date.lte=' + end_release_date + '&sort_by=primary_release_date.desc&with_release_type=3'
    
    response = requests.get(url, headers=headers)
    data_dict = json.loads(response.text)
    print(data_dict)
    
    # Extract 'id' from 'results'
    ids = pd.DataFrame.from_dict(data_dict['results'])[['id']]
    
    # Extract total_pages
    total_pages = data_dict['total_pages']
    print('total_pages: ' + str(total_pages))
    
    if total_pages > 2:
        for page in range (2, total_pages + 1):
            url = 'https://api.themoviedb.org/3/discover/movie?language=en-US&page=' + str(page) + '&primary_release_date.gte=' \
            + start_release_date + '&primary_release_date.lte=' + end_release_date + '&sort_by=primary_release_date.desc&with_release_type=3'
            response = requests.get(url, headers=headers)
            data_dict = json.loads(response.text)
            try:
                # Extract 'id' from 'results' and concat
                add_ids = pd.DataFrame.from_dict(data_dict['results'])[['id']]
                ids = pd.concat([ids, add_ids], ignore_index=True)
            except KeyError:
                print("KeyError occurred!")
                print("Contents of data_dict:", data_dict)
    return ids["id"].astype(int)

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

def movie_info_chunks(chunk:list):
    """
    Retrieves detailed information about movie from the TMDB API for one chunk of movie ids.

    Args:
        chunks (list): A list of series of movie IDs.

    Returns:
        list: A list containing the movie data.
    """
    load_dotenv()
    AUTHORIZATION = os.getenv("AUTHORIZATION") 
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AUTHORIZATION}"
    }
    logging.info("Start Thread")
    responses = []
    for movie_id in chunk:
        url = f'https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits,videos,release_dates,keywords&language=en-US'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data_dict = json.loads(response.text)
            responses.append(data_dict)
        else:
            raise Exception("Unable to retrieve TMDB data")
    return responses

def get_initial_movie_tmdb_details(file_path, start_release_date, end_release_date):
    """
    Retrieves movie details for all movie ids from TMDB API and saves the results to a NDJSON file.

    Args:
        file_path (str): The file path where the NDJSON file will be saved.

    Returns:
        None
    """
    reload(logging)
    movie_ids = get_tmdb_movie_id(start_release_date, end_release_date)
    chunks_list = chunks(movie_ids)
    movie_results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(movie_info_chunks, chunks_list)

    for result in results:
        movie_results = movie_results + result

    folder_path = file_path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    start_release_date = start_release_date.replace('-', '')
    end_release_date = end_release_date.replace('-', '')
    filename = f"raw_movie_details_{start_release_date}_{end_release_date}.ndjson"
    
    with open(os.path.join(folder_path, filename), "w") as ndjson_file:
        ndjson_file.write('\n'.join(map(json.dumps, movie_results)))
    
    print(os.path.join(folder_path, filename))

def upload_raw_initial_movie_tmdb_details_gcs():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    plugins_dir = os.path.dirname(os.path.dirname(script_dir))
    interested_dir = os.path.join(plugins_dir, "/googlecloud")
    json_path = os.path.join(interested_dir, "is3107-418809-92db84ea97f6.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    bucket_name = "movies_tmdb"

    try:   
        historicaldata_dir = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
        str_directory = os.path.join(historicaldata_dir, 'raw_historical_data/tmdb_movie')
        directory = Path(str_directory)
        filenames = list([file.name for file in directory.glob('*.ndjson')])
        delete_many_blobs(bucket_name, filenames)
        print(f"{filenames=}")
        upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
    except Exception as e:
        print(f"Error in uploading TMDB raw data to cloud storage \n Error details: {e}")

def get_raw_tmdb_movie_details_gcs():
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_movie_details")
    df =  pd.DataFrame()
    for filename in filenames:
        file_content = read_blob(bucket_name, filename)
        df = pd.concat([df, file_content], axis=0)
    return df

def clean_raw_movie_details(save_file_path:str, return_df=False):
    """
    Cleans the raw movie details from ndjson file and saves the cleaned results to a CSV file.

    Args:
        raw_file_path (str): The file path of the raw collection details NDJSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath (str) or dataframe (pd.Dataframe)
    """
    
    movie_results = get_raw_tmdb_movie_details_gcs() 
    
    selected_columns = ['budget', 'imdb_id', 'original_language', 'release_date',
                    'revenue', 'runtime', 'status']
    
    final_data = {
    'movie_id': [],
    'title': [],
    'original_language': [],
    'imdb_id': [],
    'revenue': [],
    'budget': [],
    'release_date': [],
    'runtime': [],
    'status': [],
    'production_companies_count': [],
    'is_adult': [],
    'is_adaptation': [],
    'genres': [],
    'collection_id': [],
    'cast1_id': [],
    'cast2_id': [],
    'director_id': [],
    'producer_id': [],
    'video_key_id': [],
    'tmdb_popularity': [],
    'tmdb_vote_average': [],
    'tmdb_vote_count': []
    }
    
    for index, row in movie_results.iterrows():

        is_released_in_cinema = 0
        is_continue = True
        try:
            if not pd.isna(row['release_dates']):
                for release in row['release_dates']['results']:
                    if is_continue:
                        for date in (release["release_dates"]):
                            if date['type'] == 3:
                                is_released_in_cinema = 1
                                is_continue = False
                                break
                            else:
                                continue
                    else:
                        break
        except:
            continue
        
        if is_released_in_cinema == 0:
            continue #skip movies that were not released in cinemas/theatre
            
        final_data['movie_id'].append(int(row['id']))
        final_data['title'].append(row['title'])
        final_data['is_adult'].append(0 if row['adult'] == False else 1)
        final_data['tmdb_popularity'].append(row['popularity'])
        final_data['tmdb_vote_average'].append(row['vote_average'])
        final_data['tmdb_vote_count'].append(row['vote_count'])
        
        for col in selected_columns:
            final_data[col].append(row[col])
        
        # Collection
        final_data['collection_id'].append(None if pd.isna(row['belongs_to_collection']) else int(row['belongs_to_collection']['id']))
        
        # Production Companies Count
        final_data['production_companies_count'].append(len((row['production_companies'])))
        
        # Genres
        genre_lst = [i['name']for i in row['genres']]
        final_data['genres'].append(None if len(genre_lst) == 0 else genre_lst)
        
        # Cast
        num_of_cast = len(row['credits']['cast'])
        credit_cast = (row['credits']['cast'])
        if num_of_cast >= 2:
            final_data['cast1_id'].append(int(credit_cast[0]['id']))
            final_data['cast2_id'].append(int(credit_cast[1]['id']))
        elif num_of_cast == 1:
            final_data['cast1_id'].append(int(credit_cast[0]['id']))                
            final_data['cast2_id'].append(None)
        else:                   
            final_data['cast1_id'].append(None)
            final_data['cast2_id'].append(None)
        
        # Crews
        director_id = None
        producer_id = None
        for job in (row['credits']['crew']):
            if job['job'] == 'Director':
                director_id = int(job['id'])
            elif job['job'] == 'Producer':
                producer_id = int(job['id'])

        final_data['director_id'].append(director_id)
        final_data['producer_id'].append(producer_id)
                
        # Videos key_id
        video_key_id = []
        if len(row['videos']['results']) > 0:
            for video in (row['videos']['results']):
                if video['type'] == "Trailer" or  video['type'] == "site" or  video['type'] == "Youtube":
                        video_key_id.append(video['key'])
                else:
                    continue
            final_data['video_key_id'].append(video_key_id) 
        else:
            final_data['video_key_id'].append(None)
            
        # Keywords
        if len(row['keywords']['keywords']) > 0:
            keyword_lst = [i['name']for i in (row['keywords']['keywords'])]
            contains_based_on = any('based on' in (item).lower() for item in keyword_lst)
            final_data['is_adaptation'].append(1 if contains_based_on == True else 0)
        else:
            final_data['is_adaptation'].append(0)
            
    final_df = pd.DataFrame(final_data)
    
    # Change language to its full form
    lang_url = 'https://api.themoviedb.org/3/configuration/languages'
       
    load_dotenv()
    AUTHORIZATION = os.getenv("AUTHORIZATION") 
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AUTHORIZATION}"
    }

    lang_response = requests.get(lang_url, headers=headers)
    lang_dict = json.loads(lang_response.text)
    lang_df = pd.DataFrame.from_dict(lang_dict)
    final_df = pd.merge(final_df, lang_df[['iso_639_1', 'english_name']], left_on='original_language', right_on='iso_639_1', how='left')
    
    # Drop 'original_language', 'iso_639_1'
    final_df = final_df.drop(columns=['original_language', 'iso_639_1'])
    # Rename english_name to original_language
    final_df = final_df.rename(columns={'english_name': 'original_language'})
    
    # Remove rows that don't have revenue information
    final_df = final_df[final_df['revenue'] > 0]
    
    # Convert release_date to Date
    final_df['release_date'] = pd.to_datetime(final_df['release_date']).dt.date
    
    # Rearrange columns
    final_df = final_df[['movie_id', 'revenue', 'budget', 'imdb_id', 'title', 'original_language', 'release_date', 'genres',
                        'runtime', 'status', 'production_companies_count', 'is_adult', 'is_adaptation',
                         'collection_id', 'cast1_id', 'cast2_id', 'director_id', 'producer_id', 'tmdb_popularity',
                         'tmdb_vote_average', 'tmdb_vote_count', 'video_key_id']]
    
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        final_df.to_csv(os.path.join(folder_path, "cleaned_movie_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_movie_info.csv")
    else:
        return final_df
    
def movie_ids_to_update() -> pd.Series:
    """
    Retrieves the movie IDs from movie table stored in BigQuery that was released in the past 1 month and check if there is any update in TMDB
    
    Returns:
        pd.Series: A pandas Series containing the unique new TMDB movie IDs as integers.
    """
    
    query_past_month_movie = '''
        SELECT DISTINCT CAST(movie_id AS INT64) as movie_id
        FROM `is3107-418809.movie_dataset.movie`
        WHERE (release_date) BETWEEN DATE_SUB((SELECT MAX(release_date) FROM `is3107-418809.movie_dataset.movie`), INTERVAL 1 MONTH) 
        AND (SELECT MAX(release_date) FROM `is3107-418809.movie_dataset.movie`);
    '''
    
    past_month_movie_ids = load_data_from_table(query_past_month_movie)
    past_month_movie_ids_series = pd.Series(past_month_movie_ids['movie_id'])
    
    return past_month_movie_ids_series

def get_movie_tmdb_details(start_release_date, end_release_date):
    """
    Retrieves movie details for all newly released movie ids from TMDB API and saves the results to a NDJSON file.
    
    Get new details for movies that were released in the past one month
    
    Uploaded movie details into Google Cloud Storage

    Args:
        file_path (str): The file path where the JSON file will be saved.

    Returns:
        None
    """
    # Get ids of newly released movies
    new_movie_ids = get_tmdb_movie_id(start_release_date, end_release_date)
    
    # Get new details for past month movies
    reload(logging)
    past_month_movie_ids = movie_ids_to_update()
    movie_ids = pd.concat([new_movie_ids, past_month_movie_ids], axis = 0)
    movie_ids = movie_ids.drop_duplicates() #Drop duplicates if any
    chunks_list = chunks(movie_ids)
    movie_results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(movie_info_chunks, chunks_list)

    for result in results:
        movie_results = movie_results + result
    
    #to keep a copy to google cloud storage
    script_dir = os.path.dirname(os.path.realpath(__file__))
    plugins_dir = os.path.dirname(os.path.dirname(script_dir))
    interested_dir = os.path.join(plugins_dir, "/googlecloud")
    json_path = os.path.join(interested_dir, "is3107-418809-92db84ea97f6.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    
    file_path = os.path.join(os.path.dirname(plugins_dir), "historical_data") 
    str_directory = os.path.join(file_path, 'update_data/tmdb_movie')

    folder_path = str_directory
    print(os.path.exists(folder_path))
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    start_release_date = start_release_date.replace('-', '')
    end_release_date = end_release_date.replace('-', '')
    filename = f"update_raw_movie_details_{start_release_date}_{end_release_date}.ndjson"

    with open(os.path.join(folder_path, filename), "w") as ndjson_file:
        ndjson_file.write('\n'.join(map(json.dumps, movie_results)))
    
    print(os.path.join(folder_path, filename))
     
    try:   
        bucket_name = "update_movies_tmdb"
        directory = Path(str_directory)
        filenames = list([file.name for file in directory.glob('*.ndjson')])
        delete_many_blobs(bucket_name, filenames)
        print(f"{filenames=}")
        for filename in filenames:
            upload_blob(bucket_name, os.path.join(directory, filename), filename)
        # upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
         
        #remove directory after upload
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            
    except Exception as e:
        print(f"Error in uploading TMDB raw data to cloud storage \n Error details: {e}")

def clean_new_raw_movie_details(save_file_path:str, return_df=False):
    # Get new and updated movies
    bucket_name = "update_movies_tmdb"
    filenames = list_blobs("update_movies_tmdb", prefix="update_raw_movie_details_")
    filenames.sort(key=lambda x: x.split('_')[-1].split('.')[0])
    movie_results = read_blob(bucket_name, filenames[-1])
    selected_columns = ['budget', 'imdb_id', 'original_language', 'release_date',
                'revenue', 'runtime', 'status']

    final_data = {
    'movie_id': [],
    'title': [],
    'original_language': [],
    'imdb_id': [],
    'revenue': [],
    'budget': [],
    'release_date': [],
    'runtime': [],
    'status': [],
    'production_companies_count': [],
    'is_adult': [],
    'is_adaptation': [],
    'genres': [],
    'collection_id': [],
    'cast1_id': [],
    'cast2_id': [],
    'director_id': [],
    'producer_id': [],
    'video_key_id': [],
    'tmdb_popularity': [],
    'tmdb_vote_average': [],
    'tmdb_vote_count': []
    }

    for index, row in movie_results.iterrows():

        is_released_in_cinema = 0
        is_continue = True
        try:
            if not pd.isna(row['release_dates']):
                for release in row['release_dates']['results']:
                    if is_continue:
                        for date in (release["release_dates"]):
                            if date['type'] == 3:
                                is_released_in_cinema = 1
                                is_continue = False
                                break
                            else:
                                continue
                    else:
                        break
        except:
            continue

        if is_released_in_cinema == 0:
            continue #skip movies that were not released in cinemas/theatre

        final_data['movie_id'].append(int(row['id']))
        final_data['title'].append(row['title'])
        final_data['is_adult'].append(0 if row['adult'] == False else 1)
        final_data['tmdb_popularity'].append(row['popularity'])
        final_data['tmdb_vote_average'].append(row['vote_average'])
        final_data['tmdb_vote_count'].append(row['vote_count'])

        for col in selected_columns:
            final_data[col].append(row[col])

        # Collection
        final_data['collection_id'].append(None if pd.isna(row['belongs_to_collection']) else int(row['belongs_to_collection']['id']))

        # Production Companies Count
        final_data['production_companies_count'].append(len((row['production_companies'])))

        # Genres
        genre_lst = [i['name']for i in row['genres']]
        final_data['genres'].append(None if len(genre_lst) == 0 else genre_lst)

        # Cast
        num_of_cast = len(row['credits']['cast'])
        credit_cast = (row['credits']['cast'])
        if num_of_cast >= 2:
            final_data['cast1_id'].append(int(credit_cast[0]['id']))
            final_data['cast2_id'].append(int(credit_cast[1]['id']))
        elif num_of_cast == 1:
            final_data['cast1_id'].append(int(credit_cast[0]['id']))                
            final_data['cast2_id'].append(None)
        else:                   
            final_data['cast1_id'].append(None)
            final_data['cast2_id'].append(None)

        # Crews
        director_id = None
        producer_id = None
        for job in (row['credits']['crew']):
            if job['job'] == 'Director':
                director_id = int(job['id'])
            elif job['job'] == 'Producer':
                producer_id = int(job['id'])

        final_data['director_id'].append(director_id)
        final_data['producer_id'].append(producer_id)

        # Videos key_id
        video_key_id = []
        if len(row['videos']['results']) > 0:
            for video in (row['videos']['results']):
                if video['type'] == "Trailer" or  video['type'] == "site" or  video['type'] == "Youtube":
                        video_key_id.append(video['key'])
                else:
                    continue
            final_data['video_key_id'].append(video_key_id) 
        else:
            final_data['video_key_id'].append(None)

        # Keywords
        if len(row['keywords']['keywords']) > 0:
            keyword_lst = [i['name']for i in (row['keywords']['keywords'])]
            contains_based_on = any('based on' in (item).lower() for item in keyword_lst)
            final_data['is_adaptation'].append(1 if contains_based_on == True else 0)
        else:
            final_data['is_adaptation'].append(0)

    final_df = pd.DataFrame(final_data)

    # Change language to its full form
    lang_url = 'https://api.themoviedb.org/3/configuration/languages'
    load_dotenv()
    AUTHORIZATION = os.getenv("AUTHORIZATION") 
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {AUTHORIZATION}"
    }

    lang_response = requests.get(lang_url, headers=headers)
    lang_dict = json.loads(lang_response.text)
    lang_df = pd.DataFrame.from_dict(lang_dict)
    final_df = pd.merge(final_df, lang_df[['iso_639_1', 'english_name']], left_on='original_language', right_on='iso_639_1', how='left')

    # Drop 'original_language', 'iso_639_1'
    final_df = final_df.drop(columns=['original_language', 'iso_639_1'])
    # Rename english_name to original_language
    final_df = final_df.rename(columns={'english_name': 'original_language'})

    # Remove rows that don't have revenue information
    final_df = final_df[final_df['revenue'] > 0]

    # Convert release_date to Date
    final_df['release_date'] = pd.to_datetime(final_df['release_date']).dt.date

    # Rearrange columns
    final_df = final_df[['movie_id', 'revenue', 'budget', 'imdb_id', 'title', 'original_language', 'release_date', 'genres',
                        'runtime', 'status', 'production_companies_count', 'is_adult', 'is_adaptation',
                         'collection_id', 'cast1_id', 'cast2_id', 'director_id', 'producer_id', 'tmdb_popularity',
                         'tmdb_vote_average', 'tmdb_vote_count', 'video_key_id']]
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        final_df.to_csv(os.path.join(folder_path, "cleaned_movie_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_movie_info.csv")
    else:
        return final_df