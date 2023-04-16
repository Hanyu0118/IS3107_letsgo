from google.cloud import bigquery
def config_schema_albums_op():    
    schema = [
        bigquery.SchemaField("id","STRING"),
        bigquery.SchemaField("name","STRING"),
        bigquery.SchemaField("total_tracks","INTEGER"),
        bigquery.SchemaField("available_markets","INTEGER"),
        bigquery.SchemaField("release_date","DATE"),
        bigquery.SchemaField("extract_date","DATE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config