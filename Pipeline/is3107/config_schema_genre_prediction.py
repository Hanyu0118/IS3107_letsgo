from google.cloud import bigquery
def config_schema_genre_prediction_op():    
    schema = [
    bigquery.SchemaField("id","STRING"),
    bigquery.SchemaField("Genre","STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config