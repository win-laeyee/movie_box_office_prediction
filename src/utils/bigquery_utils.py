from google.cloud import bigquery
import os

# credentials_filename = "is3107-418809-62c002a9f1f7.json"
# dataset_id = "movie_dataset"

credentials_filename = "bigquery_credentials.json"
dataset_id = "firm-catalyst-417613.IS3107"

keyfile_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../secrets", credentials_filename))
client = bigquery.Client.from_service_account_json(keyfile_path)

def query_bigquery_table(table_name):
    QUERY = f'SELECT * FROM `{dataset_id}.{table_name}`'
    try:
        query_job = client.query(QUERY) 
        data = query_job.result().to_dataframe()  
        print(data.columns)
        return data
    except Exception as e:
        print(f"Error querying table {table_name}: {e}")
        return None

def query_movie_details():
    return query_bigquery_table("movie_details")

def query_video_stats():
    return query_bigquery_table("final_clean_video_stats")

def query_collection_info():
    return query_bigquery_table("collection_info")
    # return query_bigquery_table("collection")

def query_weekly_domestic_performance():
    return query_bigquery_table("weekly_domestic_performance")

def query_people_info():
    return query_bigquery_table("people_info")

# print(query_collection_info())


# print(query_movie_details())
# Index(['movie_id', 'original_title', 'imdb_id', 'revenue', 'budget',
#        'release_date', 'runtime', 'status', 'production_companies_count',
#        'is_adult', 'is_adaptation', 'genres', 'collection_id', 'cast1_id',
#        'cast2_id', 'director_id', 'producer_id', 'video_key_id',
#        'tmdb_popularity', 'tmdb_vote_average', 'tmdb_vote_count',
#        'original_language'],
#       dtype='object')

# print(query_video_stats())
# Index(['movie_id', 'video_site', 'video_type', 'published_at', 'view_count',
#        'like_count', 'comment_count'],
#       dtype='object')

# print(query_collection_info())
# Index(['int64_field_0', 'collection_id', 'name', 'number_movies_before_2020',
#        'avg_popularity_before_2020'],
#       dtype='object')


# print(query_weekly_domestic_performance())
# Index(['int64_field_0', 'week_end_date', 'id', 'rank', 'domestic_gross',
#        'domestic_theaters_count'],
#       dtype='object')


# print(query_people_info())
# Index(['people_id', 'name', 'birthday', 'gender', 'tmdb_popularity',
#        'known_for', 'total_number_cast_credits', 'total_number_crew_credits'],
#       dtype='object')