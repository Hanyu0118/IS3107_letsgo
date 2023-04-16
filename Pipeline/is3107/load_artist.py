from .connect_to_bigquery import connect_to_bigquery_op
from .config_schema_artist import config_schema_artist_op
# from .duplicates_table import create_duplicates_table_op
from google.cloud import bigquery

def load_artist_op(clean_new_artists):
    schema, job_config = config_schema_artist_op()
    client = connect_to_bigquery_op()
    table_id = "snappy-boulder-378707.NewReleases.Artists"
    
    # create_duplicates_table_op(config_schema_artist_op, table_id)

    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in table: ", original_rows)

    # Load
    if original_rows == 0:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = client.load_table_from_dataframe(clean_new_artists, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    rows_after_loading = table.num_rows

    print(
        "Loaded {} rows and {} columns to {}".format(
            (rows_after_loading - original_rows), len(table.schema), table_id
        )
    )
    print("Total rows in table after loading: ", rows_after_loading)
    return