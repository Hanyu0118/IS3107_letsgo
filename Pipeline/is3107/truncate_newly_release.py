from .connect_to_bigquery import connect_to_bigquery_op
from google.cloud import bigquery

def truncate_newly_release_op():
    client = connect_to_bigquery_op()

    #Artist
    delete_records_in_table = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.NewReleases.Artists
    """)
    delete_records_in_table.result()
    print("Artist for newly release Deleted")

    #album
    delete_records_in_table = client.query("""
       TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewAlbums
    """)
    delete_records_in_table.result()
    print("Album for newly release Deleted")

    #track info
    delete_records_in_table = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewTracks
    """)
    delete_records_in_table.result()
    print("TrackInfo for newly release Deleted")

    #audio feature
    delete_records_in_table = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.NewReleases.NewAudioFeatures
    """)
    delete_records_in_table.result()
    print("Audio feature for newly release Deleted")





    