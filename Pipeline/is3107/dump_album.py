from .connect_to_bigquery import connect_to_bigquery_op

def dump_album_op():
    client = connect_to_bigquery_op()
    
    #Check current rows
    table_id = "snappy-boulder-378707.TrackClearInfo.AlbumInfo"
    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in album info table: ", original_rows)
    
    #Dump album info
    dump_album = client.query("""
       INSERT INTO snappy-boulder-378707.TrackClearInfo.AlbumInfo 
       SELECT id,name,total_tracks,release_date,extract_date, available_markets 
       FROM snappy-boulder-378707.NewReleases.NewAlbums
    """)
    dump_album.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after dump in album info table: ", rows)
    
    #Delete newly realease albums info
    table_id_newly = "snappy-boulder-378707.NewReleases.NewAlbums"
    table = client.get_table(table_id_newly)
    rows_newly = table.num_rows
    delete_records_in_table = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewAlbums
    """)
    delete_records_in_table.result()
    print("Check whether all albums dumped: ", (rows-original_rows) == rows_newly)