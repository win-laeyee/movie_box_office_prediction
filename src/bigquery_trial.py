from google.cloud import bigquery
import os
import pandas as df
import streamlit as st
from google.oauth2 import service_account

## need {project_id}.{dataset_id}.{table_id} to perform queries

def read_data():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gbq_service_account"]
    )
    client = bigquery.Client(credentials=credentials)

    QUERY = (
        'SELECT * FROM `movie_dataset.movie` '
        'LIMIT 100')
    query_job = client.query(QUERY) # API request
    data = query_job.result().to_dataframe()   # Waits for query to finish and write into a df

    print(data.columns)
    return data

def create_model():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gbq_service_account"]
    )
    client = bigquery.Client(credentials=credentials)

    QUERY = (
        """
        CREATE MODEL `movie_dataset.model_v6`
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

def predict_revenue(budget, release_date, runtime, is_adult, is_adaptation, cast1_popularity, cast2_popularity, director_popularity, producer_popularity, view_count, like_count, comment_count, avg_popularity_before_2020):

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gbq_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
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

    return data.iloc[0,0]

def find_value(df_name, match_value, match_column, return_column):
    output = df_name.loc[df_name[match_column] == match_value, return_column].values
    if len(output) > 0:
        return output[0]
    else:
        return None
    
    

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