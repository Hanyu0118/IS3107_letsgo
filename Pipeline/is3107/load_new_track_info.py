# from .connect_to_bigquery import connect_to_bigquery_op
# from .config_schema_audio_features import config_schema_audio_features_op
# from .audio_features_duplicates_table import create_audio_features_duplicates_table_op, delete_audio_features_duplicates_table_op
# from google.cloud import bigquery

# def load_new_track_info_op():
#     return

# from google.cloud import bigquery
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "letsgo-snappy-boulder-378707-4b7d46801fd1.json"
# # Construct a BigQuery client object.
# client = bigquery.Client()

# table_id = "snappy-boulder-378707.NewReleases.NewTracks"
# job = client.load_table_from_dataframe(clean_new_tracks, table_id, job_config=job_config)

# job.result()  # Waits for the job to complete.

# table = client.get_table(table_id)  # Make an API request.
# print(
#     "Loaded {} rows and {} columns to {}".format(
#         table.num_rows, len(table.schema), table_id
#     )
# )