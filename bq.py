import os

from google.cloud import bigquery
from buckets import get_blobs
from time import sleep


def new_dataset(project_id, dataset_id):
    client = bigquery.Client()
    full_dataset_id = f"{ client.project }.{ dataset_id }"
    new_dataset = bigquery.Dataset(full_dataset_id)
    new_dataset = client.create_dataset(new_dataset)
    print(f"Created dataset { client.project }.{ new_dataset.dataset_id }")


def csv_to_bq(table_name, uri):
    """load csv from gcp bucket to bq with auto schema detection
    
    Arguments:
        table_name {str} -- table name
        uri {str} -- uri of csv in gcp bucket
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"

    j = client.load_table_from_uri(uri, t, job_config=job_config)

    while not j.done():
        print(f"working on { j.job_id }...")
        sleep(2)
    if j.errors:
        print(f"errors with { t }, { j.errors }")
    else:
        print(f"done with { t }")


if __name__ == "__main__":
    project_id = "chnd-demo"
    dataset_id = "claims_md"
    new_dataset(project_id, dataset_id)

    my_bucket = f"gs://chnd-demo-bucket"
    file_names = [blob.name for blob in get_blobs("chnd-demo-bucket") if blob.name[-4:] == ".csv"]

    table_dict = {} # table_name: bucket_uri
    for f in file_names:
        full_table_id = f"{ project_id }.{ dataset_id }.{ f[7:-4] }"
        table_dict[full_table_id] = os.path.join(my_bucket, f)

    for t in table_dict.keys():
        csv_to_bq(t, table_dict[t])
