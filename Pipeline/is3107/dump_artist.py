from .connect_to_bigquery import connect_to_bigquery_op

def dump_artist_op():
    client = connect_to_bigquery_op()

    #Check current rows
    table_id = "snappy-boulder-378707.History.Artists"
    table = client.get_table(table_id)
    original_rows = table.num_rows
    print("Total rows in table: ", original_rows)

    #Delete duplicates in history
    delete_duplicates = client.query("""
        DELETE FROM snappy-boulder-378707.History.Artists
        WHERE id IN (
            SELECT t1.id
            FROM snappy-boulder-378707.NewReleases.Artists t1
            INNER JOIN snappy-boulder-378707.History.Artists t2
            ON t1.id = t2.id
        )
    """)

    delete_duplicates.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after deletion in table: ", rows)

    #Dump info
    dump_album = client.query("""
        INSERT INTO snappy-boulder-378707.History.Artists 
        SELECT id,name,followers,popularity 
        FROM snappy-boulder-378707.NewReleases.Artists
    """)
    dump_album.result()
    table = client.get_table(table_id)
    rows= table.num_rows
    print("Total rows after dump in album info table: ", rows)
    return