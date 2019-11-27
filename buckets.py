import os

from google.cloud import storage

# os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


def list_buckets():
    """Lists all buckets."""
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        print(bucket.name)

# list_buckets()


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)

    return blobs

# blobs = list_blobs("chnd-demo-bucket")

def get_blobs(bucket_name):
    storage_client = storage.Client()

    return storage_client.list_blobs(bucket_name)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


# upload_blob("chnd-demo-bucket", "data/driver.csv", "claims/driver.csv")
