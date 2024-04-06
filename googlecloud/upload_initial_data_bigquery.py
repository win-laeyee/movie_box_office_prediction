from google.cloud import bigquery
import os

#may want create a dag to upload clean initial data to bigquery
def upload_csv_to_table(project_id, dataset_id, table_id, csv_file_path, mode):
    """mode can be (append, truncate or empty)"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecloud/is3107-418809-62c002a9f1f7.json"

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # If CSV file has a header row, skip it
    if mode == "append":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == "truncate":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif mode == "empty":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY  # Write only when tables empty - ensure no any overwrite

    # Load data from CSV file into the table
    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f"CSV file {csv_file_path} uploaded to table {table_id} in dataset {dataset_id} successfully.")

def delete_all_data_from_table(project_id, dataset_id, table_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecloud/is3107-418809-62c002a9f1f7.json"

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Construct SQL query to delete all rows from the table
    sql_query = f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table_id}`"

    # Execute the SQL query
    query_job = client.query(sql_query)
    query_job.result()  # Wait for the query to complete

    print(f"All data deleted from table {table_id} in dataset {dataset_id}.")


if __name__ == "__main__":
    project_id = "is3107-418809"
    dataset_id = "movie_dataset"
    csv_file_path = os.path.join("extraction/clean_initial_data", "cleaned_weekly_domestic_performance.csv")

    # testing only upload clean initial weekly domestic performance to the table
    delete_all_data_from_table(project_id, dataset_id, "weekly_domestic_performance")
    upload_csv_to_table(project_id, dataset_id, "weekly_domestic_performance", csv_file_path, "empty")