from google.cloud import bigquery
import os
import pandas as df

import sys
sys.path.append('/Users/liusiyi/Desktop/IS3107/Project/repo/movie_box_office_prediction/src')



# for access to bigquery data base, need to generate a service account
# download the service account credentials in json format
# this would be used to access bigquery
# change the keyfile_path to where you store ur service account credentials
keyfile_path = "secrets/is3107-418809-62c002a9f1f7.json"


######## Bigquery Access method samples

## need {project_id}.{dataset_id}.{table_id} to perform queries

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
    
    # alternative create model query to be used, R-squared is much higher
    """"
      CREATE MODEL `firm-catalyst-417613.IS3107.test_model_4`
      OPTIONS(model_type = 'RANDOM_FOREST_REGRESSOR')
      AS 
      SELECT
      revenue as label,
      budget,
      release_date,
      runtime,
      cast1_popularity,
      cast2_popularity,
      director_popularity,
      producer_popularity,
      tmdb_popularity,
      tmdb_vote_average,
      tmdb_vote_count,
      -- published_at,
      view_count,
      like_count,
      comment_count,
      avg_popularity_before_2020
      from
      IS3107.training_v1
    """
    query_job = client.query(QUERY)
    output = query_job.result()
    print(output)

def prep_data_for_ml():
    client = bigquery.Client.from_service_account_json(keyfile_path)
    QUERY = (
        """ 
        SELECT
        revenue,
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

def predict_revenue(budget, release_date, runtime, is_adult, is_adaptation, cast1_popularity, cast2_popularity, director_popularity, producer_popularity, view_count, like_count, comment_count, avg_popularity_before_2020):
    client = bigquery.Client.from_service_account_json(keyfile_path)
    # budget = 350000

    ## Prepare input parameters
    # make sure that all None values are replaced by "null" such that the SQL would work
    input_params = [budget, release_date, runtime, is_adult, is_adaptation, cast1_popularity, cast2_popularity, director_popularity, producer_popularity, view_count, like_count, comment_count, avg_popularity_before_2020]
    input_params = ["null" if item is None else item for item in input_params]
    print(release_date)
    print(input_params[1])
    print(type(input_params[1]))
    QUERY = (
        f"""
        SELECT
          predicted_label as predicted_revenue
        FROM
        ML.PREDICT(MODEL `movie_dataset.model_v6`, (
          SELECT
            {input_params[0]} AS budget,
            cast('{input_params[1]}' AS DATE FORMAT 'YYYY-MM-DD') AS release_date,
            {input_params[2]} AS runtime,
            {input_params[3]} AS is_adult,
            {input_params[4]} AS is_adaptation,
            {input_params[5]} AS cast1_popularity,
            {input_params[6]} AS cast2_popularity,
            {input_params[7]} AS director_popularity,
            {input_params[8]} AS producer_popularity,
            {input_params[9]} AS view_count,
            {input_params[10]} AS like_count,
            {input_params[11]} AS comment_count,
            {input_params[12]} AS avg_popularity_before_2020))
        """
    )
    query_job = client.query(QUERY) # API request
    data = query_job.result().to_dataframe()   # Waits for query to finish and write into a df

    # print(data.iloc[0,0])
    return data.iloc[0,0]

def find_value(df_name, match_value, match_column, return_column):
    output = df_name.loc[df_name[match_column] == match_value, return_column].values
    if len(output) > 0:
        return output[0]
    else:
        return None
    
    

# try creating a model directly using movie_details data
# predict_revenue(350000)

"""
Apparently Bigquery free service does not include DML queries, 
so cannot insert, delete and update table
"""


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

###### Bigquery Code for Prediction ######
## Query to predict revenue use an already trained model, can manually subs in the data input for prediction
"""
SELECT
  predicted_label as predicted_revenue
FROM
ML.PREDICT(MODEL `IS3107.test_model_4`, (
  SELECT
    50000 AS budget,
    cast(null as date) AS release_date,
    124 AS runtime,
    10 AS cast1_popularity,
    null AS cast2_popularity,
    6 AS director_popularity,
    1.2 AS producer_popularity,
    25 AS tmdb_popularity,
    7 AS tmdb_vote_average,
    1789 AS tmdb_vote_count,
    2209901 AS view_count,
    17584 AS like_count,
    1019 AS comment_count,
    null AS avg_popularity_before_2020))
"""