import os
import pandas as pd
import numpy as np
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from googlecloud.read_data_gcs import read_blob, list_blobs
from googleapiclient.discovery import build


def get_video_keys_gcs() -> pd.DataFrame:
    """
    Retrieves the video collection IDs and details from files stored in Google Cloud Storage (GCS).

    Returns:
        pd.DataFrame: Pandas DataFrame containing the video keys (and details) needed to call YouTube/Vimeo APIs.
    """
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_movie_details")
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
    return [item for item in response["items"]]

def get_vimeo_video_stats(chunk: list) -> list:
    """
    Retrieves video statistics from Vimeo for one chunk of keys at a time.

    Args:
        chunks (list): A list of series of video keys.

    Returns:
        response (list): List containing Vimeo video statistics data as records.
    """
    load_dotenv()
    VIMEO_API_TOKEN = os.getenv("VIMEO_API_TOKEN")
    print(VIMEO_API_TOKEN)
    results = []
    headers = {
        'Authorization': f'Bearer {VIMEO_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    for video_key in chunk:
        api_url = f'https://api.vimeo.com/videos/{video_key}?fields=stats,metadata'
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

    return results

def get_clean_video_stats(save_file_path: str, return_df=False):
    """
    Cleans the raw movie details from ndjson files and saves the cleaned results after extracting video details into a CSV file.

    Args:
        raw_file_path (str): The file path of the raw collection details JSON file.
        save_file_path (str): The directory path where the cleaned CSV file will be saved.

    Returns:
        filepath(str) or dataframe (pd.DataFrame)
    """

    video_key_df = get_video_keys_gcs()
    
    # filter for different video sites
    vimeo_video_keys = video_key_df[video_key_df["site"] == "Vimeo"]["key"]
    youtube_video_keys = video_key_df[video_key_df["site"] == "YouTube"]["key"]
    
    # chunk data
    vimeo_chunks_list = chunks(vimeo_video_keys)
    youtube_chunks_list = chunks(youtube_video_keys)

    pass
    


if __name__ == "__main__":
    video_key_df = get_video_keys_gcs()
    vimeo_video_keys = video_key_df[video_key_df["site"] == "Vimeo"]["key"]
    vimeo_chunks_list = chunks(vimeo_video_keys)
    results = []