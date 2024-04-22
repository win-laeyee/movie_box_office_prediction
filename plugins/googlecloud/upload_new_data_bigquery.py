import os
from google.cloud import bigquery
from create_table_bigquery import create_table_if_not_exists
from datetime import datetime, timedelta


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
    
    # Construct temporary table based on schema
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)
    expiration_time = datetime.now() + timedelta(days=1) # Set the table expiration time
    table.expires = expiration_time
    create_table_if_not_exists(project_id, dataset_id, table_id, schema)
    
    # Define job configuration
    job_config = bigquery.LoadJobConfig()
    if mode == "append":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == "truncate":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif mode == "empty":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY  # Write only when tables empty - ensure no any overwrite

    df['insertion_datetime'] = datetime.now() # create new column if not exist and save to same file
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result() # Wait for the job to complete
    print(f"Dataframe uploaded to temporary table {table_id} in dataset {dataset_id} successfully.")

def upsert_df_to_table(project_id, dataset_id, table_id, primary_key_columns, df):
    """
    Upsert a pandas Dataframe to the specified BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): ID of the BigQuery table.
        primary_key_columns (list of str): List of column names that form the primary key.
        df (pandas.DataFrame): Dictionary containing column names and their corresponding values for the new row.

    Returns:
        None
    """

    # Initialize BigQuery client
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    client = bigquery.Client(project=project_id)
    table_dest = f"{project_id}.{dataset_id}.{table_id}"
    table_src = f"{project_id}.staging_dataset.{table_id}"

    # Add insertion datetime column to row
    df["insertion_datetime"] = datetime.now()

    # Construct SQL merge statement
    source_columns = df.columns.tolist()
    target_columns = list(filter(lambda i: i not in primary_key_columns, source_columns))
    merge_sql = f"""
        MERGE `{table_dest}` AS target
        USING `{table_src}` AS source
        ON {', '.join([f'target.{col} = source.{col}' for col in primary_key_columns])}        
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in target_columns])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(source_columns)})
            VALUES ({', '.join(source_columns)})
    """
    
    # Get original table schema
    original_table_ref = client.dataset(dataset_id).table(table_id)
    original_table = client.get_table(original_table_ref)
    original_table_schema = original_table.schema

    # Populate temp table
    upload_df_to_temp_table(project_id, "staging_dataset", table_id, original_table_schema, df, mode="truncate")

    # Execute SQL merge statement
    query_job = client.query(merge_sql)
    query_job.result()
    print("Upsert operation completed successfully.")