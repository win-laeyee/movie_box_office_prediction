import os
import time
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
from googlecloud.read_data_gcs import read_blob, list_blobs
from googlecloud.upload_initial_data_gcs import upload_many_blobs_with_transfer_manager, upload_blob
from googleapiclient.discovery import build
from airflow.exceptions import AirflowNotFoundException


def get_video_keys_gcs(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Retrieves the video collection IDs and details from files stored in Google Cloud Storage (GCS), based on start and end dates.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the video keys (and details) needed to call YouTube/Vimeo APIs.
    """
    bucket_name = "update_movies_tmdb"
    print(f"update_raw_movie_details_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")
    filenames = list_blobs("update_movies_tmdb", prefix=f"update_raw_movie_details_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")
    if not filenames:
        raise AirflowNotFoundException("Update movie details raw JSON files not found!")
    
    df = pd.DataFrame()
    for filename in tqdm(filenames):
        file_content = read_blob(bucket_name, filename)[["id", "videos"]].rename(columns={"id": "movie_id"})
        file_content = file_content.dropna(subset=["videos"]).astype({"movie_id": int, "videos": object})
        file_content["videos"] = file_content["videos"].apply(lambda x: x["results"])
        file_content = file_content.explode("videos").dropna(subset=["videos"])
        file_content = pd.concat([file_content["movie_id"].reset_index(drop=True), pd.json_normalize(file_content["videos"])], axis=1)
        df = pd.concat([df, file_content], axis=0)
    return df

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

def get_youtube_video_stats(chunk: list) -> list:
    """
    Retrieves video statistics from YouTube for one chunk of keys at a time.

    Args:
        chunks (list): A list of series of video keys.

    Returns:
        response (list): List containing YouTube video statistics data as records.
    """
    load_dotenv()
    YOUTUBE_API_TOKEN = os.getenv("YOUTUBE_API_TOKEN")
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=YOUTUBE_API_TOKEN)
    request = youtube.videos().list(
        part="statistics",
        id=chunk
    )
    response = request.execute()
    print(response)

    results = [item for item in response["items"]]
    return results

def get_vimeo_video_stats(keys: list) -> list:
    """
    Retrieves video statistics from Vimeo for one chunk of keys at a time.

    Args:
        keys (list): A list of video keys.

    Returns:
        results (list): List containing Vimeo video statistics data as records.
    """
    load_dotenv()
    VIMEO_API_TOKEN = os.getenv("VIMEO_API_TOKEN")

    results = []
    RATE_LIMIT = 50 # Vimeo API rate limit, as of Apr 2024
    rate_limit_count = 0

    for video_key in tqdm(keys):
        api_url = f'https://api.vimeo.com/videos/{video_key}?fields=stats,metadata'
        headers = {
            'Authorization': f'Bearer {VIMEO_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        response = requests.get(api_url, headers=headers)
        record = {"video_key_id": video_key}

        # Check if request was successful (status code 200)
        if response.status_code == 200:
            video_data = response.json()
            record["view_count"] = video_data["stats"]["plays"]
            record["like_count"] = video_data["metadata"]["connections"]["likes"]["total"]
            record["comment_count"] = video_data["metadata"]["connections"]["comments"]["total"]
        else:
            print(f"Failed to retrieve video data. Status code: {response.status_code}")

        results.append(record)
        rate_limit_count += 1

        if rate_limit_count >= RATE_LIMIT: # every X counts scraped, sleep for 45 seconds
            time.sleep(45)
            rate_limit_count = 0

    return results

def extract_raw_video_stats(raw_file_dir: str, start_date: datetime, end_date: datetime):
    """
    Cleans the raw movie details from ndjson files and saves the cleaned results after extracting video details into a CSV file.
    Upload the CSV file into Google Cloud Storage.
    
    Args:
        raw_file_dir (str): The absolute directory path where the raw collection details CSV file will be saved.
        start_date (datetime): Datetime object of start date from which to filter.
        end_date (datetime): Datetime object of end date to which to filter.

    Returns:
        None
    """
    # get video keys from YouTube and Vimeo APIs
    video_key_df = get_video_keys_gcs(start_date, end_date)
    
    # filter for different video sites
    vimeo_video_keys = video_key_df[video_key_df["site"] == "Vimeo"]["key"]
    youtube_video_keys = video_key_df[video_key_df["site"] == "YouTube"]["key"]
    
    # create raw file storage directory if not exists
    if not os.path.exists(raw_file_dir):
        os.makedirs(raw_file_dir)

    # fetch vimeo data
    vimeo_results = get_vimeo_video_stats(vimeo_video_keys)
    if vimeo_results:
        vimeo_df = pd.DataFrame(vimeo_results)
        vimeo_df.to_csv(os.path.join(raw_file_dir, f"raw_vimeo_video_stats_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"), index=False)

    # fetch youtube data
    youtube_chunks_list = chunks(youtube_video_keys)
    youtube_results = []
    for chunk in youtube_chunks_list:
        youtube_results.extend(get_youtube_video_stats(chunk.tolist()))
    if youtube_results:
        youtube_statistics = [{"id": result["id"], **result["statistics"]} for result in youtube_results]
        youtube_df = pd.DataFrame(youtube_statistics)
        youtube_df.to_csv(os.path.join(raw_file_dir, f"raw_youtube_video_stats_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"), index=False)

    # upload to gcs
    filenames = list([file.name for file in Path(raw_file_dir).glob(f"raw_*_video_stats_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv")])
    for filename in filenames:
        upload_blob("update_movies_tmdb", os.path.join(Path(raw_file_dir), filename), filename)