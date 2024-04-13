import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import date, datetime
from googlecloud.read_data_gcs import read_blob, list_blobs
from googlecloud.upload_initial_data_gcs import delete_many_blobs, upload_many_blobs_with_transfer_manager
from dotenv import load_dotenv
import logging
import requests
from importlib import reload
import concurrent.futures
import json

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
    Cleans the raw movie details from ndjson file and saves the cleaned results to a CSV file or return a data frame.

    Args:
        raw_file_path (str): The file path of the raw collection details NDJSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath (str) or dataframe (pd.Dataframe)
    """
    
    movie_results = get_raw_tmdb_movie_details_gcs() #How do I only get new data in google cloud storage
    
    selected_columns = ['budget', 'imdb_id', 'original_language', 'original_title', 'release_date',
                    'revenue', 'runtime', 'status']
    
    final_data = {
    'movie_id': [],
    'original_title': [],
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
            
        final_data['movie_id'].append(row['id'])
        final_data['is_adult'].append(0 if row['adult'] == False else 1)
        final_data['tmdb_popularity'].append(row['popularity'])
        final_data['tmdb_vote_average'].append(row['vote_average'])
        final_data['tmdb_vote_count'].append(row['vote_count'])
        
        for col in selected_columns:
            final_data[col].append(row[col])
        
        # Collection
        final_data['collection_id'].append(None if pd.isna(row['belongs_to_collection']) else row['belongs_to_collection']['id'])
        
        # Production Companies Count
        final_data['production_companies_count'].append(len((row['production_companies'])))
        
        # Genres
        genre_lst = [i['name']for i in row['genres']]
        final_data['genres'].append(None if len(genre_lst) == 0 else genre_lst)
        
        # Cast
        num_of_cast = len(row['credits']['cast'])
        credit_cast = (row['credits']['cast'])
        if num_of_cast >= 2:
            final_data['cast1_id'].append(str(credit_cast[0]['id']))
            final_data['cast2_id'].append(str(credit_cast[1]['id']))
        elif num_of_cast == 1:
            final_data['cast1_id'].append(str(credit_cast[0]['id']))                
            final_data['cast2_id'].append(None)
        else:                   
            final_data['cast1_id'].append(None)
            final_data['cast2_id'].append(None)
        
        # Crews
        director_id = None
        producer_id = None
        for job in (row['credits']['crew']):
            if job['job'] == 'Director':
                director_id = str(job['id'])
            elif job['job'] == 'Producer':
                producer_id = str(job['id'])

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
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI5ODJmYTNkYWU1OGY4Y2U1ZmU2M2Q1NmI5Njk2ZDk2MCIsInN1YiI6IjY1ZmQ1ZjRlMjI2YzU2MDE2NDZlZGU2NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.9KniXks8_611yzqRn1AsGD6mKOhtD2bJ6twLrH8FoUo"
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
    
    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        cleaned_df.to_csv(os.path.join(folder_path, "cleaned_movie_info.csv"), index=False)
        return os.path.join(folder_path, "cleaned_movie_info.csv")
    else:
        return final_df