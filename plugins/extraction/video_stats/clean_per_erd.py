import os
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from datetime import datetime
from googlecloud.read_data_gcs import read_blob, list_blobs
from googleapiclient.discovery import build


def get_raw_video_details_gcs(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Retrieves the raw video details from raw movie details data stored in Google Cloud Storage (GCS).

    Args:
        start_date (datetime, optional): Datetime object of start date from which to filter.
        end_date (datetime, optional): Datetime object of end date to which to filter.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the video keys and additional video details.
    """
    bucket_name = "movies_tmdb"
    prefix = "raw_movie_details"
    if start_date and end_date:
        prefix = prefix + f"_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    filenames = list_blobs(bucket_name, prefix=prefix)
    df = pd.DataFrame()
    for filename in tqdm(filenames):
        file_content = read_blob(bucket_name, filename)[["id", "videos"]].rename(columns={"id": "movie_id"})
        file_content = file_content.dropna(subset=["videos"]).astype({"movie_id": int, "videos": object})
        file_content["videos"] = file_content["videos"].apply(lambda x: x["results"])
        file_content = file_content.explode("videos").dropna(subset=["videos"])
        file_content = pd.concat([file_content["movie_id"].reset_index(drop=True), pd.json_normalize(file_content["videos"])], axis=1)
        df = pd.concat([df, file_content], axis=0)
    return df.drop_duplicates()

def get_raw_video_statistics_gcs(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Retrieves the raw video statistics data stored in Google Cloud Storage (GCS).

    Args:
        start_date (datetime, optional): Datetime object of start date from which to filter.
        end_date (datetime, optional): Datetime object of end date to which to filter.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the video keys and statistics.
    """
    bucket_name = "movies_tmdb"
    sites = ["YouTube", "Vimeo"]
    df = pd.DataFrame()
    for site in sites:
        logging.info(f"Getting raw data for {site}...")
        prefix = f"raw_{site.lower()}_video_stats"
        if start_date and end_date:
            prefix = prefix + f"_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        filenames = list_blobs(bucket_name, prefix=prefix)
        for filename in tqdm(filenames):
            file_content = read_blob(bucket_name, filename)
            if site == "YouTube": # Extra transformations needed for YouTube statistics
                file_content = file_content[["id", "viewCount", "likeCount", "commentCount"]]
                file_content = file_content.rename(columns={"id": "video_key_id", "viewCount": "view_count", "likeCount": "like_count", "commentCount": "comment_count"})
            df = pd.concat([df, file_content], axis=0)
    return df.drop_duplicates()

def clean_raw_video_statistics(save_file_path: str, start_date: datetime = None, end_date: datetime = None, return_df=False):
    """
    Cleans the raw movie details and statistics from CSV files and saves the cleaned results to a CSV file.

    Args:
        save_file_path (str): The directory path where the cleaned CSV file will be saved.
        start_date (datetime, optional): Datetime object of start date from which to filter.
        end_date (datetime, optional): Datetime object of end date to which to filter.
        return_df (boolean, default False): If True, return dataframe. Else, save to `save_file_path`. 

    Returns:
        filepath (str) or dataframe (pd.DataFrame)
    """
    video_details_df = get_raw_video_details_gcs(start_date, end_date)
    video_statistics_df = get_raw_video_statistics_gcs(start_date, end_date)

    combined_df = video_details_df[["movie_id", "key", "site", "type", "published_at"]].set_index("key").join(video_statistics_df.set_index("video_key_id"), how="inner")
    combined_df.reset_index(drop=True, inplace=True)
    combined_df.rename(columns={"site": "video_site", "type": "video_type"}, inplace=True)
    combined_df.drop_duplicates(inplace=True)

    if not return_df:
        folder_path = save_file_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        combined_df.to_csv(os.path.join(folder_path, "clean_video_stats.csv"), index=False)
        return os.path.join(folder_path, "clean_video_stats.csv")
    else:
        return combined_df
    
