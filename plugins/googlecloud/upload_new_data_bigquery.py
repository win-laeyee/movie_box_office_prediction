import os
from google.cloud import bigquery
from datetime import datetime

def upload_df_to_temp_table(project_id, dataset_id, table_id, schema, df, mode):
    """
    Uploads a pandas DataFrame to a temporary BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        df (pandas.DataFrame): The DataFrame to be uploaded.
        mode (str): The write mode for the table. Possible values are "append", "truncate", or "empty".

    Returns:
        None
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    client = bigquery.Client(project=project_id)
    
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_id, schema=schema)
    # Set the table expiration time
    expiration_time = datetime.utcnow() + timedelta(days=1)  
    table.expires = expiration_time
    table = client.create_table(table, timeout=30)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Define job configuration
    job_config = bigquery.LoadJobConfig()
    if mode == "append":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == "truncate":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif mode == "empty":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY  # Write only when tables empty - ensure no any overwrite

    df['insertion_datetime'] = datetime.now() # create new column if not exist and save to same file
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Wait for the job to complete

    print(f"Dataframe uploaded to table {table_id} in dataset {dataset_id} successfully.")

    
def upsert_row_to_table(project_id, dataset_id, table_id, primary_key_columns, new_row_values):
    """
    Upsert a row in the specified BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): ID of the BigQuery table.
        primary_key_columns (list of str): List of column names that form the primary key.
        new_row_values (dict): Dictionary containing column names and their corresponding values for the new row.

    Returns:
        None
    """

    # Initialize BigQuery client
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    client = bigquery.Client(project=project_id)
    table_dest = f"{project_id}.{dataset_id}.{table_id}"

    # Construct query to check if row already exists
    where_conditions = ' AND '.join(f"{col} = @{col}" for col in primary_key_columns)
    select_query = f"SELECT * FROM `{table_dest}` WHERE {where_conditions}"

    # Set query parameters
    query_params = [bigquery.ScalarQueryParameter(name, "STRING", value) for name, value in new_row_values.items()]
    
    # Run the select query
    query_job = client.query(select_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    existing_row = query_job.result()

    # Add insertion datetime column to row
    new_row_values['insertion_datetime'] = datetime.now()

    if existing_row:
        # If the row exists, update it
        update_values = ', '.join(f"{col} = @{col}" for col in new_row_values.keys())
        update_query = f"UPDATE `{table_dest}` SET {update_values} WHERE {where_conditions}"

        # Run the update query
        query_job = client.query(update_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
        print("Row updated successfully.")
    else:
        # If the row doesn't exist, insert it
        columns = ', '.join(new_row_values.keys())
        values = ', '.join(f"@{col}" for col in new_row_values.keys())
        insert_query = f"INSERT INTO `{table_dest}` ({columns}) VALUES ({values})"

        # Run the insert query
        query_job = client.query(insert_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
        print("Row inserted successfully.")