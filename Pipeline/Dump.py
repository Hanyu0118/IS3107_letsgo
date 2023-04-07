# [START import_module]
from datetime import datetime
from datetime import timedelta
from textwrap import dedent
import pandas as pd
import json
from dateutil import parser

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# Tasks from other python file
# import sys 
# sys.path.insert(1, "../is3107")
# print(sys.path)
import os
path=os.getcwd()

from is3107.dump_album import dump_album_op
from is3107.dump_track_info import dump_trackinfo_op
from is3107.dump_audio_features import dump_audiofeatures_op
from is3107.dump_genre_populariy import dump_genre_popularity_op

# [END import_module]


# [START instantiate_dag]
with DAG(
    'DumpInfo',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(seconds=5),

    },
    # [END default_args]
    description='SpotifyPlus DumpInfo ETL',
    schedule_interval=None,
    start_date=datetime(2023, 3, 21),
    catchup=False,
    tags=['is3107_project'],
) as dag:
    # [END instantiate_dag]

    # [START documentation]
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    # [END documentation]

    # [START  dump_album]
    def dump_album(**kwargs):
        ti = kwargs['ti']
        albums =  dump_album_op()
        print("Dump albums succeed: ", albums)
    # [END  dump_album]

    # [START  dump_trackinfo]
    def dump_trackinfo(**kwargs):
        ti = kwargs['ti']
        trackinfo =  dump_trackinfo_op()
        print("Dump trackinfo succeed: ", trackinfo)
    # [END  dump_trackinfo]

    # [START  dump_album]
    def dump_audiofeatures(**kwargs):
        ti = kwargs['ti']
        audiofeatures =  dump_audiofeatures_op()
        print("Dump albums succeed: ", audiofeatures)
    # [END  dump_album]
    
    # [START  dump_genrepopulariy]
    def dump_genre_populariy(**kwargs):
        ti = kwargs['ti']
        genrepopulariy =  dump_genre_popularity_op()
        print("Dump albums succeed: ", genrepopulariy)
    # [END  dump_genrepopulariy]