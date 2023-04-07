from .connect_to_bigquery import connect_to_bigquery_op
import pandas as pd
from .config_schema_genrepopularity import config_schema_genrepopularity_op
from google.cloud import bigquery

def dump_genre_popularity_op():
    client = connect_to_bigquery_op()
    
    #Get popularity table
    table_id = "snappy-boulder-378707.TrackClearInfo.TrackClearInfo"
    popularity= client.get_table(table_id)
    popularity = popularity[["id", "popularity", "extract_date"]]

    #Get genre table
    table_id = "snappy-boulder-378707.TrackGenre.Trackgenre"
    genre= client.get_table(table_id)
    
    #merge two and get statistics
    genre = genre.melt(id_vars=['track_id'], var_name='genre', value_name='value')
    genre = genre[genre.value == 1]
    genre.drop(['value'], axis=1, inplace=True)
    genre_popularity = pd.merge(popularity, genre, left_on = "id", right_on = "track_id", how="left")
    stats = genre_popularity.groupby(['genre','extract_date']).aggregate({'popularity':'sum','id':'count'}).reset_index()
    stats.columns = ['genre','extract_date','popularity','total_tracks']
    
    #Create table in big query
    schema, job_config = config_schema_genrepopularity_op()
    
    table_id = "snappy-boulder-378707.GenrePopularity.GenrePopularityMonth"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, timeout=30)
     
    #Dump
    job = client.load_table_from_dataframe(stats, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    
    #insert into table
    dump_genre_popularity =  client.query("""
        INSERT INTO snappy-boulder-378707.GenrePopularity.GenrePopularity
        SELECT * FROM snappy-boulder-378707.GenrePopularity.GenrePopularityMonth
    """)
    dump_genre_popularity.result()
    
    #drop temporary table
    dump_genre_popularity =  client.query("""
        DROP TABLE snappy-boulder-378707.GenrePopularity.GenrePopularityMonth
    """)
    dump_genre_popularity.result()

    return table.num_rows == 19
    

