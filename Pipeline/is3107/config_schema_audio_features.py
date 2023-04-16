from google.cloud import bigquery
def config_schema_audio_features_op():    
    schema = [
        bigquery.SchemaField("id","STRING"),
        bigquery.SchemaField("danceability","FLOAT"),
        bigquery.SchemaField("energy","FLOAT"),
        bigquery.SchemaField("key","INTEGER"),
        bigquery.SchemaField("loudness","FLOAT"),
        bigquery.SchemaField("mode","INTEGER"),
        bigquery.SchemaField("speechiness","FLOAT"),
        bigquery.SchemaField("acousticness","FLOAT"),
        bigquery.SchemaField("instrumentalness","FLOAT"),
        bigquery.SchemaField("liveness","FLOAT"),
        bigquery.SchemaField("valence","FLOAT"),
        bigquery.SchemaField("tempo","FLOAT"),
        bigquery.SchemaField("duration_ms","INTEGER"),
        bigquery.SchemaField("time_signature","INTEGER"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config