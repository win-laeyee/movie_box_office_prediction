from google.cloud import bigquery
import os

keyfile_path = "../secrets/bigquery_credentials.json"
# client = bigquery.Client()
client = bigquery.Client.from_service_account_json(keyfile_path)

# Perform a query.
QUERY = (
    'SELECT * FROM `firm-catalyst-417613.IS3107.movie_details` '
    'LIMIT 100')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(type(row))
    print(row.original_title)

print(type(rows))