from .connect_to_bigquery import connect_to_bigquery_op

def dump_audiofeatures_op():
    client = connect_to_bigquery_op()
    
    #Check current rows
    table_id = "snappy-boulder-378707.AudioFeatures.AudioFeatures"
    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in audio features table: ", original_rows)
    
    #Delete duplicates in history
    delete_duplicates = client.query("""
        DELETE FROM snappy-boulder-378707.AudioFeatures.AudioFeatures
        WHERE id IN (
            SELECT t1.id
            FROM snappy-boulder-378707.NewReleases.NewAudioFeatures t1
            INNER JOIN snappy-boulder-378707.AudioFeatures.AudioFeatures t2
            ON t1.id = t2.id
        )
    """)
    delete_duplicates.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after deletion in album info table: ", rows)

    #Dump album info
    dump_album = client.query("""
        INSERT INTO snappy-boulder-378707.AudioFeatures.AudioFeatures 
        SELECT danceability, energy,key,loudness,mode,speechiness,
        acousticness,instrumentalness,liveness,valence,	
        tempo,id, duration_ms,time_signature 
        FROM snappy-boulder-378707.NewReleases.NewAudioFeatures
    """)
    dump_album.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after dump in audio features table: ", rows)
    
    #Delete newly realease albums info
    table_id_newly = "snappy-boulder-378707.NewReleases.NewAudioFeatures"
    table = client.get_table(table_id_newly)
    rows_newly = table.num_rows
    # delete_records_in_table = client.query("""
    #     TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewAudioFeatures
    # """)
    # delete_records_in_table.result()
    print("Check whether all audio features dumped: ", (rows-original_rows) == rows_newly)
    return (rows-original_rows) == rows_newly
