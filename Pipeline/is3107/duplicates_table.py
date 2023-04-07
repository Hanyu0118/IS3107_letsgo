from .connect_to_bigquery import connect_to_bigquery_op
from .config_schema_audio_features import config_schema_audio_features_op
from .config_schema_albums import config_schema_albums_op
from google.cloud import bigquery

def create_duplicates_table_op(op, table_id):
    client = connect_to_bigquery_op()
    schema, job_config = op()

    # TODO(developer): Set table_id to the ID of the table to create.
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, timeout=30)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    return

def delete_audio_features_duplicates_table_op(table_id):
    client = connect_to_bigquery_op()
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))
    return