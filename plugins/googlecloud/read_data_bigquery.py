from google.cloud import bigquery
import os
import pandas as pd
from datetime import datetime
import logging

def load_data_from_table(query, project_id="is3107-418809") -> pd.DataFrame:
    """
    Loads data from a BigQuery table using provided query.

    Args:
        query (str): The SQL query to execute for BigQuery.
        project_id (str: The ID of the Google Cloud project.

    Returns:
        pandas.DataFrame: The loaded data as a pandas DataFrame.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-92db84ea97f6.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    client = bigquery.Client(project=project_id)

    try:
        df = client.query(query).to_dataframe()
        logging.info('Retrieved successfully query: {query}')
    except Exception as e:
        raise Exception(f"Error in loading data: {e}")

    return df