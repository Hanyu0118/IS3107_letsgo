from .connect_to_bigquery import connect_to_bigquery_op

def dump_trackinfo_op():
    client = connect_to_bigquery_op()
    
    #Check current rows
    table_id = "snappy-boulder-378707.History.Tracks"
    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in track info table: ", original_rows)
    
    #Delete duplicates in history
    delete_duplicates = client.query("""
        DELETE FROM snappy-boulder-378707.History.Tracks
        WHERE id IN (
            SELECT t1.id
            FROM snappy-boulder-378707.NewReleases.NewTracks t1
            INNER JOIN snappy-boulder-378707.History.Tracks t2
            ON t1.id = t2.id
        )
    """)
    delete_duplicates.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after deletion in album info table: ", rows)
    
    #Dump album info
    dump_album = client.query("""
        INSERT INTO snappy-boulder-378707.History.Tracks
        SELECT id, name, album_id, artist_id, popularity, explicit, available_markets,extract_date
        FROM snappy-boulder-378707.NewReleases.NewTracks
    """)
    dump_album.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after dump in track info table: ", rows)
    
    #Delete newly realease albums info
    table_id_newly = "snappy-boulder-378707.NewReleases.NewTracks"
    table = client.get_table(table_id_newly)
    rows_newly = table.num_rows
