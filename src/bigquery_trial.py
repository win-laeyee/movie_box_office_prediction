from google.cloud import bigquery
import os
import pandas as df


# for access to bigquery data base, need to generate a service account
# download the service account credentials in json format
# this would be used to access bigquery
# change the keyfile_path to where you store ur service account credentials
keyfile_path = "../secrets/bigquery_credentials.json"


######## Bigquery Access method samples

def read_data():
    # start a bigquery client using ur service account credentials
    client = bigquery.Client.from_service_account_json(keyfile_path)
    # Perform a query.
    QUERY = (
        'SELECT * FROM `firm-catalyst-417613.IS3107.movie_details` '
        'LIMIT 100')
    query_job = client.query(QUERY) # API request
    data = query_job.result().to_dataframe()   # Waits for query to finish and write into a df

    print(data.columns)
    return data

def create_model():
    client = bigquery.Client.from_service_account_json(keyfile_path)
    QUERY = (
        """
        CREATE MODEL `firm-catalyst-417613.IS3107.test_model_2`
        OPTIONS(model_type = 'LINEAR_REG')
        AS 
        SELECT
        revenue as label,
        budget,
        release_date,
        runtime,
        tmdb_vote_count,
        cast(cast1_id as string) as cast1_id,
        cast(cast2_id as string) as cast2_id,
        cast(director_id as string) as director_id,
        cast(producer_id as string) as producer_id
        from
        IS3107.movie_details
        """)
    query_job = client.query(QUERY)
    output = query_job.result()
    print(output)

create_model()

"""
CREATE MODEL `firm-catalyst-417613.IS3107.test_model_1`
OPTIONS(model_type = 'RANDOM_FOREST_REGRESSOR')
AS 
SELECT
  revenue as label,
  budget,
  release_date,
  runtime,
  tmdb_vote_count,
  cast(cast1_id as string) as cast1_id,
  cast(cast2_id as string) as cast2_id,
  cast(director_id as string) as director_id,
  cast(producer_id as string) as producer_id
from
  IS3107.movie_details

"""
