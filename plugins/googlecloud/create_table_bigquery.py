from google.cloud import bigquery
import os

def create_dataset_if_not_exists(project_id, dataset_id):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define dataset reference
    dataset_ref = client.dataset(dataset_id)

    try:
        dataset = client.get_dataset(dataset_ref)   # Check if the dataset exists
        print(f"Dataset {dataset_id} already exists. Skipping creation.")
    except Exception:
        # Dataset does not exist, create it
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"  # Specify the location for the dataset
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created successfully.")

def create_table_if_not_exists(project_id, dataset_id, table_id, schema):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)    # Check if the table exists
        print(f"Table {table_id} already exists. Skipping creation.")
    except Exception as e:

        # Define table metadata
        table = bigquery.Table(table_ref, schema=schema)

        # Create the table
        try:
            client.create_table(table)  # API request
            print(f"Table {table_id} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_id}: {e}")

def create_movie_table(project_id, dataset_id):
    table_id = "movie"
    schema = [
        bigquery.SchemaField("movie_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("revenue", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("budget", "INT64"),
        bigquery.SchemaField("imdb_id", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("original_language", "STRING"),
        bigquery.SchemaField("release_date", "DATE"),
        bigquery.SchemaField("runtime", "INT64"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("production_companies_count", "INT64"),
        bigquery.SchemaField("is_adult", "BOOL"),
        bigquery.SchemaField("is_adaptation", "BOOL"),
        bigquery.SchemaField("genres", "STRING", mode="REPEATED"),  
        bigquery.SchemaField("collection_id", "INT64"),
        bigquery.SchemaField("cast1_id", "INT64"),
        bigquery.SchemaField("cast2_id", "INT64"),
        bigquery.SchemaField("director_id", "INT64"),
        bigquery.SchemaField("producer_id", "INT64"),
        bigquery.SchemaField("video_key_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tmdb_popularity", "FLOAT64"),
        bigquery.SchemaField("tmdb_vote_average", "FLOAT64"),
        bigquery.SchemaField("tmdb_vote_count", "FLOAT64")
    ]
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)

def create_collection_table(project_id, dataset_id):
    table_id = "collection"
    schema = [
        bigquery.SchemaField("collection_id", "INT64", mode="REQUIRED"), 
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("number_movies_before_2020", "INT64"),
        bigquery.SchemaField("avg_tmdb_popularity_before_2020", "FLOAT64")
    ]
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)

def create_people_table(project_id, dataset_id):
    table_id = "people"
    schema = [
        bigquery.SchemaField("people_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("birthday", "DATE"),
        bigquery.SchemaField("known_for", "STRING"),
        bigquery.SchemaField("tmdb_popularity", "FLOAT64"),
        bigquery.SchemaField("total_number_cast_credits", "INT64"),
        bigquery.SchemaField("total_number_crew_credits", "INT64")
    ]
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)

def create_video_stats_table(project_id, dataset_id):
    table_id = "video_stats"
    schema = [
        bigquery.SchemaField("video_key_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("video_type", "STRING"),
        bigquery.SchemaField("video_site", "STRING"),
        bigquery.SchemaField("view_count", "INT64"),
        bigquery.SchemaField("like_count", "INT64"),
        bigquery.SchemaField("favourite_count", "INT64"),
        bigquery.SchemaField("comment_count", "INT64")
    ]
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)

def create_weekly_domestic_performance_table(project_id, dataset_id):
    table_id = "weekly_domestic_performance"
    schema = [
        bigquery.SchemaField("week_end_date", "DATE", mode="REQUIRED"),  # Make week_end_date mandatory
        bigquery.SchemaField("movie_id", "INT64", mode="REQUIRED"),      # Make movie_id mandatory
        bigquery.SchemaField("rank", "INT64"),
        bigquery.SchemaField("domestic_gross", "INT64"),
        bigquery.SchemaField("domestic_theaters_count", "INT64")
    ]
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)


if __name__ == "__main__":
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    create_dataset_if_not_exists(project_id, dataset_id)
    create_movie_table(project_id, dataset_id)
    create_collection_table(project_id, dataset_id)
    create_people_table(project_id, dataset_id)
    create_video_stats_table(project_id, dataset_id)
    create_weekly_domestic_performance_table(project_id, dataset_id)