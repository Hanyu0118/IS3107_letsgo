from .connect_to_bigquery import connect_to_bigquery_op
from .config_schema_albums import config_schema_albums_op
# from .duplicates_table import create_duplicates_table_op
from google.cloud import bigquery

def load_new_albums_op(clean_albums):
    schema, job_config = config_schema_albums_op()
    client = connect_to_bigquery_op()
    table_id = "snappy-boulder-378707.NewReleases.NewAlbums"
    table_id_dup = "snappy-boulder-378707.NewReleases.NewAlbumsDuplicates"
    
    # create_duplicates_table_op(config_schema_albums_op, table_id_dup)

    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in table: ", original_rows)

    # Load
    if original_rows == 0:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = client.load_table_from_dataframe(clean_albums, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    rows_after_loading = table.num_rows

    print(
        "Loaded {} rows and {} columns to {}".format(
            (rows_after_loading - original_rows), len(table.schema), table_id
        )
    )
    print("Total rows in table after loading: ", rows_after_loading)
    
    
    # # Load duplicates into a duplicate table
    # identify_duplicates = client.query("""
    #     INSERT INTO snappy-boulder-378707.NewReleases.NewAlbumsDuplicates
    #     WITH first_row AS (
    #         SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY extract_date DESC) AS row_number, *
    #         FROM snappy-boulder-378707.NewReleases.NewAlbums
    #     )
    #     SELECT id, name, total_tracks, available_markets, release_date, extract_date
    #     FROM first_row
    #     WHERE row_number = 1
    # """)
    # identify_duplicates.result()
    # table = client.get_table(table_id_dup)
    # rows_dup = table.num_rows
    # print("Total rows in duplicates table: ", rows_dup)

    # # Delete all duplicate records in the orginial table
    # delete_duplicates = client.query("""
    #     DELETE FROM snappy-boulder-378707.NewReleases.NewAlbums
    #     WHERE id
    #     IN (SELECT DISTINCT id FROM snappy-boulder-378707.NewReleases.NewAlbumsDuplicates)
    # """)
    # delete_duplicates.result()
    # table = client.get_table(table_id)
    # rows_after_deletion = table.num_rows
    # print("Total rows deleted in table: ", rows_after_loading - rows_after_deletion)
    # print("Total rows in table after deleting duplicates: ", rows_after_deletion)

    # # Insert one copy of every duplicate record back
    # insert_back = client.query("""
    #     INSERT INTO snappy-boulder-378707.NewReleases.NewAlbums
    #     SELECT * FROM snappy-boulder-378707.NewReleases.NewAlbumsDuplicates
    # """)
    # insert_back.result()
    # table = client.get_table(table_id)
    # rows_after_inserting_back = table.num_rows
    # print("Total rows in table after inserting back: ", rows_after_inserting_back)
    
    # # Delete records in the duplicate table
    # table = client.get_table(table_id_dup)
    # rows_dup = table.num_rows
    # delete_records_in_duplicates_table = client.query("""
    #     TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewAlbumsDuplicates
    # """)
    # delete_records_in_duplicates_table.result()
    # table = client.get_table(table_id_dup)
    # rows_after_deletion_dup = table.num_rows
    # print("Total rows deleted in duplicates table: ", rows_dup - rows_after_deletion_dup)
    
    return