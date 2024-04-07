from google.cloud import storage
from io import BytesIO
from pathlib import Path
import pandas as pd
import os


def list_blobs(bucket_name, prefix=None):
    """Lists files in a Google Cloud Storage bucket"""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    # Iterate through the blobs and print their names and creation time
    # for blob in blobs:
    #     print(f"File: {blob.name}, Creation Time: {blob.time_created}")

    return [blob.name for blob in blobs]


def read_blob(bucket_name, blob_name):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    
    """Reads the contents of a blob from the Google Cloud Storage bucket."""
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Get the blob
    blob = bucket.blob(blob_name)

    # Download the blob's content as a string
    content = BytesIO(blob.download_as_string())

    if blob_name.endswith(".csv"):
        df = pd.read_csv(content)
    elif blob_name.endswith(".ndjson"):
        df = pd.read_json(content, lines=True)
    elif blob_name.endwith(".json"):
        df = pd.read_json(content)

    return df