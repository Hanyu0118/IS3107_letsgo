from google.cloud import bigquery
def config_schema_genrepopularity_op():    
    schema = [
        bigquery.SchemaField("genre","STRING"),
        bigquery.SchemaField("extract_date","DATE"),
        bigquery.SchemaField("popularity","INTEGER"),
        bigquery.SchemaField("total_tracks","INTEGER")
    ]

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    return schema, job_config