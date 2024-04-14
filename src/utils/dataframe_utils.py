import pandas as pd
import ast 
import numpy as np
from src.utils.bigquery_utils import query_movie_details, query_video_stats, query_collection_info, query_weekly_domestic_performance, query_people_info
from src.utils.cache_utils import query_or_load_from_cache

def get_movie_details():
    df = query_or_load_from_cache(query_movie_details, "movie_details")
    # df = pd.read_csv('../is3107_data/clean_movie_details.csv')
    return df

def get_video_stats():
    df = query_or_load_from_cache(query_video_stats, "clean_video_stats")
    # df = pd.read_csv('../is3107_data/clean_video_stats.csv')
    return df

def get_collection_info():
    df = query_or_load_from_cache(query_collection_info, "collection_info")
    # df = pd.read_csv('../is3107_data/cleaned_collection_info.csv')
    return df

def get_weekly_domestic_performance():
    df = query_or_load_from_cache(query_weekly_domestic_performance, "weekly_domestic_performance")
    # df = pd.read_csv('../is3107_data/cleaned_weekly_domestic_performance.csv')
    return df

def get_people_info():
    df = query_or_load_from_cache(query_people_info, "people_info")
    # df = pd.read_csv('../is3107_data/people_info.csv')
    return df


def get_top_5_movies():
    movie_details_df = get_movie_details()
    top_movies = movie_details_df.sort_values(by='revenue', ascending=False).head(5)
    columns_to_display = [
        'original_title', 'genres', 'revenue',
        'tmdb_popularity', 'tmdb_vote_average'
    ]
    top_movies_display = top_movies[columns_to_display]
    column_names_to_display = ['Movie Title', 'Genres', 'Revenue', 'TMDB Popularity', 'TMDB Vote Average']
    top_movies_display.columns = column_names_to_display
    return top_movies_display


def merge_movie_weekly_performance():
    df_time = get_weekly_domestic_performance()
    df_rev = get_movie_details()
    merged_df = pd.merge(df_time, df_rev, left_on='id', right_on='movie_id', how='inner', suffixes=('_time', '_rev'))
    # merged_df = merged_df.drop(columns=['Unnamed: 0'])
    # merged_df = merged_df.drop(columns=['id'])
    return merged_df

def include_profit_in_df(df):
    df['profit'] = df['revenue'] - df['budget']
    df = df[df['budget'] > 0] 
    return df



def get_rev_over_time(rev_or_profit):
    merged_df = merge_movie_weekly_performance()   

    merged_df['week_end_date'] = pd.to_datetime(merged_df['week_end_date'])
    
    merged_df['genres'] = merged_df['genres'].apply(ast.literal_eval)
    df_exploded = merged_df.explode('genres')
    df_exploded = df_exploded.reset_index(drop=True)

    df_exploded = include_profit_in_df(df_exploded)

    if rev_or_profit == 'Revenue':
        genre_revenue_over_time = df_exploded.groupby(['genres', pd.Grouper(key='week_end_date', freq='W')])['revenue'].sum().reset_index()
    else:
        genre_revenue_over_time = df_exploded.groupby(['genres', pd.Grouper(key='week_end_date', freq='W')])['profit'].sum().reset_index()
    genre_revenue_over_time.dropna(inplace=True)


    return genre_revenue_over_time


def get_all_unique_genres():
    movie_df = get_movie_details()
    movie_df = movie_df.dropna(subset=['genres'])

    movie_df['genres'] = movie_df['genres'].apply(ast.literal_eval)
    unique_genres = set(genre for sublist in movie_df['genres'] for genre in sublist)

    return unique_genres


def get_popularity_over_time(popularity_metric):
    merged_df = merge_movie_weekly_performance()   

    merged_df['week_end_date'] = pd.to_datetime(merged_df['week_end_date'])
    
    merged_df['genres'] = merged_df['genres'].apply(ast.literal_eval)
    df_exploded = merged_df.explode('genres')
    df_exploded = df_exploded.reset_index(drop=True)

    if popularity_metric == 'tmdb_vote_count':
        genre_popularity_over_time = df_exploded.groupby(['genres', pd.Grouper(key='week_end_date', freq='W')])[popularity_metric].sum().reset_index()
    else:
        genre_popularity_over_time = df_exploded.groupby(['genres', pd.Grouper(key='week_end_date', freq='W')])[popularity_metric].mean().reset_index()
        genre_popularity_over_time[popularity_metric] = genre_popularity_over_time[popularity_metric].round(0).astype(int)

    return genre_popularity_over_time


def merge_movie_people_dir_and_prod(person):
    movie_df = get_movie_details()
    people_df = get_people_info()

    known_for_mapping = {'director_id': 'Directing', 'producer_id': 'Production'}
    dir_and_prof_people_df = people_df[people_df['known_for'] == known_for_mapping[person]]

    merged_df = pd.merge(movie_df, dir_and_prof_people_df, left_on=person, right_on='people_id', how='inner')

    return merged_df



def calculate_director_producer_profit_margin(person):
    person_mapping = {'Director': 'director_id', 'Producer': 'producer_id'}
    movie_people_df = merge_movie_people_dir_and_prod(person_mapping[person])
    movie_people_df['profit'] = movie_people_df['revenue'] - movie_people_df['budget']
    movie_people_df = movie_people_df[movie_people_df['budget'] > 0]  
    movie_people_df['profit_margin'] = movie_people_df['profit'] / movie_people_df['budget']

    director_profit_margin = movie_people_df.groupby(person_mapping[person])['profit_margin'].mean().reset_index()
    director_profit_margin = pd.merge(director_profit_margin, movie_people_df[[person_mapping[person], 'name']], on=person_mapping[person], how='left')
    return director_profit_margin


def merge_movie_collection():
    movie_df = get_movie_details()
    collection_df = get_collection_info()

    merged_df = pd.merge(movie_df, collection_df, left_on='collection_id', right_on='collection_id', how='inner')
    # merged_df = merged_df.drop(columns=['Unnamed: 0'])
    return merged_df



def merge_movie_people_actors():
    movie_df = get_movie_details()
    people_df = get_people_info()

    acting_people_df = people_df[people_df['known_for'] == 'Acting']
    
    merged_df = pd.merge(movie_df, acting_people_df, left_on='cast1_id', right_on='people_id', how='left', suffixes=('_cast1', '_cast2'))
    merged_df = merged_df.rename(columns={'name': 'cast1_name'})

    merged_df = pd.merge(merged_df, acting_people_df, left_on='cast2_id', right_on='people_id', how='left', suffixes=('_cast1', '_cast2'))
    merged_df = merged_df.rename(columns={'name': 'cast2_name'})

    merged_df = merged_df.drop(columns=['people_id_cast1', 'people_id_cast2'])


    return merged_df


def calculate_avg_rev_by_actor():
    df = merge_movie_people_actors()
    df_melted = df.melt(id_vars='revenue', value_vars=['cast1_name', 'cast2_name'], value_name='actor').drop('variable', axis=1)

    df_actor_revenue = df_melted.groupby('actor')['revenue'].mean().reset_index()
    # df_actor_revenue['log_revenue'] = np.log(df_actor_revenue['revenue']) 
    return df_actor_revenue


def calculate_roi():
    df = get_movie_details()
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce').replace(0, np.nan)
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

    df['ROI'] = (df['revenue'] - df['budget']) / df['budget']
    return df

def merge_movie_video_stats():
    movie_df = get_movie_details()
    video_stats_df = get_video_stats()
    merged_df = pd.merge(movie_df, video_stats_df, left_on='movie_id', right_on='movie_id', how='inner')
    return merged_df

