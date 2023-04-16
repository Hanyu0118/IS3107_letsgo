from google.cloud import bigquery
def config_schema_artist_op():   

    schema = [
        bigquery.SchemaField("id","STRING"),
        bigquery.SchemaField("name","STRING"),
        bigquery.SchemaField("followers","INTEGER"),
        bigquery.SchemaField("popularity","INTEGER"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config
