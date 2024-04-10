#https://www.educative.io/answers/how-to-upload-a-file-to-google-cloud-storage-on-python-3

import os
from google.cloud import storage
from pathlib import Path
from google.cloud.storage import Client, transfer_manager


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the Google Cloud Storage bucket."""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Define the blob object
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")



def upload_many_blobs_with_transfer_manager(bucket_name, filenames, source_directory="", workers=8):
    """Upload every file in a list to a bucket, concurrently in a process pool.

    Each blob name is derived from the filename, not including the
    `source_directory` parameter. For complete control of the blob name for each
    file (and other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(filenames, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.
        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))


def delete_many_blobs(bucket_name, blob_names):
    """Deletes multiple blobs from the bucket."""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for blob_name in blob_names:
        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.delete()
            print("Blob {} deleted.".format(blob_name))
        else:
            print("Blob {} does not exist.".format(blob_name))


#initialise the gcs (not updating)
if __name__ == "__main__":
    # Set the path to your service account key file
    script_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(script_dir, "is3107-418809-62c002a9f1f7.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    bucket_name = "movies_tmdb"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_names = [blob.name for blob in bucket.list_blobs()]

    try:
        #raw tmdb file (joanne)
        str_directory = './historical_data/raw_historical_data/tmdb'
        directory = Path(str_directory)
        filenames = list([file.name for file in directory.glob('*.ndjson')])
        delete_many_blobs(bucket_name, filenames)
        print(f"{filenames=}")
        upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
    except Exception as e:
        print("Error in uploading TMDB raw data to cloud storage")
        print(f"Error details: {e}")

    # try:
    #     #boxoffice mojo raw (shantia)
    #     str_directory = './historical_data/raw_historical_data/boxofficemojo'
    #     directory = Path(str_directory)
    #     filenames = list([file.name for file in directory.glob('*.csv')])
    #     delete_many_blobs(bucket_name, filenames)
    #     upload_many_blobs_with_transfer_manager(bucket_name, filenames=filenames, source_directory=str_directory)
    # except Exception:
    #     print("Error in uploading boxofficemojo raw data to cloud storage")




    