from google.cloud import storage
from io import BytesIO
from pathlib import Path
import pandas as pd
import json
import os

def list_blobs_object(bucket_name, prefix=None):
    """Lists files in a Google Cloud Storage bucket"""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-92db84ea97f6.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def list_blobs(bucket_name, prefix=None):
    """Lists files in a Google Cloud Storage bucket"""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-92db84ea97f6.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    # Iterate through the blobs and print their names and creation time
    # for blob in blobs:
    #     print(f"File: {blob.name}, Creation Time: {blob.time_created}")

    return [blob.name for blob in blobs]


def read_blob(bucket_name, blob_name, json_as_dict=False):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-92db84ea97f6.json")
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
    elif blob_name.endswith(".json") and not json_as_dict:
        df = pd.read_json(content)
    elif blob_name.endswith(".json") and json_as_dict:
        content.seek(0) # Reset the file pointer to the start
        df = json.load(content)

    return df