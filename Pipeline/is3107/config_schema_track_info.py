from google.cloud import bigquery
def config_schema_tracks_info_op():    
    schema = [
    bigquery.SchemaField("id","STRING"),
    bigquery.SchemaField("name","STRING"),
    bigquery.SchemaField("explicit","INTEGER"),
    bigquery.SchemaField("available_markets","INTEGER"),
    bigquery.SchemaField("popularity","INTEGER"),
    bigquery.SchemaField("album_id","STRING"),
    bigquery.SchemaField("artist_id","STRING"),
    bigquery.SchemaField("extract_date","DATE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config