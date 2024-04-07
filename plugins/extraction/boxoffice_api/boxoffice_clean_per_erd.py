import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import date, datetime
from googlecloud.read_data_gcs import read_blob, list_blobs
from google.cloud import storage
from io import BytesIO



# Raw data files are on google cloud storage, this is the transformation done
def get_tmdb_date_id_title_gcs():
    """Get Raw TMDB Data from GCS and transform (release_date, id, title)"""
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="raw_movie_details")
    interested_col = ['release_date', 'id', 'title']
    df =  pd.DataFrame()
    #read file from gcs and get interested col
    for filename in filenames:
        file_content = read_blob(bucket_name, filename)[interested_col]
        df = pd.concat([df, file_content], axis=0)
    df["title_cleaned"] = df["title"].str.strip().str.replace(r'\W+', ' ', regex=True).str.lower()

    return df

def first_friday(year):
    """Get first friday of the year"""
    for i in range(1,8):
        date_obj = date(year, 1, i)
        if date_obj.weekday() == 4:
            return datetime.strftime(date_obj, '%Y-%m-%d')
        
def last_date(year):
    """Get last date of year"""
    return f"{year}-12-31"

def get_weeks_end_date(start_year:int=2021, end_year:int=2030):
    """Get end dates of week according to boxofficemojo definition"""
    df = pd.DataFrame()
    for i in range(start_year, end_year):
        period_week = pd.period_range(start=first_friday(i), end=last_date(i), freq='W-THU') #original calender interested in week 2 to week 1 of next year
        df = pd.concat([df, pd.DataFrame({"period_week": period_week[0:52], "week": np.arange(1,53)})])
    df["year"] = df["period_week"].dt.year
    df["week_end_date"] = df["period_week"].dt.end_time.dt.date
    return df[["year", "week", "week_end_date"]]

def get_boxofficemojo_data_gcs():
    """Get Raw Box Office Mojo Data from GCS and transform (release_date, id, title)"""
    bucket_name = "movies_tmdb"
    filenames = list_blobs("movies_tmdb", prefix="boxofficemojo_data")
    df =  pd.DataFrame()
    for filename in filenames:
        file_content = read_blob(bucket_name, filename)
        df = pd.concat([df, file_content], axis=0)
    
    #cleaning
    df["is_rerelease"] = df["Release"].str.contains('(?:.+\d{4}\sRe-release)|(?:.+\d{2}th\sAnniversary)|(?:.+4K\sRestoration)', regex=True).astype(int)
    df["title_cleaned"] = df["Release"].str.strip().str.replace(r'\W+', ' ', regex=True).str.lower()
    df["gross"] = df["Gross"].str.replace(',', '').str.replace('$', '').astype(int)
    df["theaters"] = df["Theaters"].str.replace(',', '').str.replace(r'^-$', '0', regex=True).astype(int)
    df["Weeks"] = df["Weeks"].astype(str).str.replace(r'^-$', '0', regex=True).fillna('0').astype(int)
    columns_of_interest = ["year", "week", "title_cleaned", "Rank", "gross", "theaters", 'Weeks']
    subdf = df.loc[df["is_rerelease"] == 0, columns_of_interest].copy()

    #get more information on week end date and likely release date (to solve for multiple movies with the same name)
    intermmediate_df = subdf.merge(get_weeks_end_date(), on=['year', 'week'], how='inner')
    intermmediate_df['likely_release_date'] = intermmediate_df.apply(lambda row: row['week_end_date'] - pd.Timedelta(weeks=row['Weeks']+1), axis=1)

    return intermmediate_df

#Main function
def get_clean_weekly_domestic_performance(data_path:str):
    """
    Cleans and processes the weekly domestic performance data for movies from raw data from google cloud storage.

    Args:
        data_path (str): The path to the folder where the cleaned data will be saved.

    Returns:
        str: The path to the cleaned data file.
    """
    df = get_tmdb_date_id_title_gcs()
    intermmediate_df = get_boxofficemojo_data_gcs()

    final_df = intermmediate_df.merge(df, on='title_cleaned', how='left')
    final_df = final_df.dropna(subset=['id']) #drop those that we can't find a match (state as not available)
    final_df['likely_release_date'] = pd.to_datetime(final_df['likely_release_date'])
    final_df['release_date'] = pd.to_datetime(final_df['release_date'])
    final_df['days_diff'] = (final_df['likely_release_date'] - final_df['release_date']).dt.days.abs()
    final_df.columns = final_df.columns.str.lower()
    final_df = final_df.loc[final_df.groupby(['title_cleaned', 'week_end_date'])['days_diff'].idxmin()].sort_values(['week_end_date', 'rank']) #by title and week, find most likely id
    final_df['id'] = final_df['id'].astype(int, errors='ignore')

    interested_final = ['week_end_date', 'id', 'rank', 'gross', 'theaters']

    folder_path = data_path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    #save one checking file and another final clean file -- should push clean to big query
    #final_df.loc[final_df['days_diff']<=50].to_csv('check.csv', index=False)

    #choose 50 as cut off (increasing days diff increases uncertainty of correct matches)
    (final_df.loc[final_df['days_diff']<=50,interested_final]
    .rename(columns={'id': 'movie_id', 'gross':'domestic_gross', 'theaters': 'domestic_theaters_count'})
    .to_csv(os.path.join(folder_path, f"cleaned_weekly_domestic_performance.csv"), index=False))

    return str(os.path.join(folder_path, f"cleaned_weekly_domestic_performance.csv"))

if __name__ == "__main__":
    #get_clean_weekly_domestic_performance()
    pass