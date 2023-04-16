import os
from google.cloud import bigquery

def connect_to_bigquery_op():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "letsgo-snappy-boulder-378707-4b7d46801fd1.json"
    # Construct a BigQuery client object.
    client = bigquery.Client()
    return client
