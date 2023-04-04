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

from is3107.extract_new_releases import extract_new_releases_op #Task1  checked
from is3107.clean_new_albums import clean_new_albums_op #Task2 checked
from is3107.load_new_albums import load_new_albums_op # Task3
from is3107.extract_new_albums_full_info import extract_new_albums_full_info_op #Task4 checked
from is3107.extract_artist import extract_artist_op #Task5 checked
from is3107.get_new_track_ids import get_new_track_ids_op #Task6 checked
from is3107.extract_new_track_info import extract_new_track_info_op #Task7 checked
from is3107.extract_audio_features import extract_audio_features_op #Task8 checked
from is3107.clean_new_track_info import clean_new_track_info_op #Task9 checked
from is3107.load_new_track_info import load_new_track_info_op # Task10
from is3107.clean_audio_features import clean_audio_features_op #Task11 checked
from is3107.load_audio_features import load_audio_features_op #Task12
from is3107.clean_artist import clean_artist_op #Task13 checked
from is3107.load_artist_to_history import load_artist_to_history_op #Task14



# [END import_module]


# [START instantiate_dag]
with DAG(
    'SpotifyPlus',
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
    description='SpotifyPlus ETL',
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

    # [START extract_new_releases]
    def extract_new_releases(**kwargs):
        ti = kwargs['ti']
        albums, extract_date_album = extract_new_releases_op()
        with open('albums.json', 'w', encoding='utf-8') as f:
            json.dump(albums, f, ensure_ascii=False, indent=4)
        print("ALBUM EXTRACTED")
        ti.xcom_push('extract_date_album', str(extract_date_album))
    # [END extract_new_releases]

    # [START clean_new_albums]
    def clean_new_albums(**kwargs):
        ti = kwargs['ti']
        extract_date_album = ti.xcom_pull(task_ids='extract_new_releases', key='extract_date_album')
        extract_date_album = parser.parse(extract_date_album)
        with open('albums.json', 'r') as f:
            albums = json.load(f)
        clean_albums = clean_new_albums_op(albums, extract_date_album)
        clean_albums.to_csv("clean_new_albums.csv", index=False)
        print("ALBUM CLEANED")
    # [END clean_new_albums]

    # [START extract_new_albums_full_info]
    def extract_new_albums_full_info(**kwargs):
        ti = kwargs['ti']
        with open('clean_albums.json', 'r') as f:
            clean_albums = json.load(f)
        albums_full_info = extract_new_albums_full_info_op(clean_albums)
        with open('albums_full_info.json', 'w', encoding='utf-8') as f2:
            json.dump(albums_full_info, f2, ensure_ascii=False, indent=4)
        print("ALBUM FULL INFO EXTRACTED")
    # [END extract_new_albums_full_info]

    # [START get_new_track_ids]
    def get_new_track_ids(**kwargs):
        ti = kwargs['ti']
        with open('clean_albums.json', 'r') as f:
            clean_albums = json.load(f)
        clean_albums = pd.DataFrame(clean_albums)
        clean_albums.reset_index(drop=True, inplace=True)
        clean_albums['release_date'] = clean_albums['release_date'].apply(parser.parse)
        clean_albums['extract_date'] = clean_albums['extract_date'].apply(parser.parse)
        with open ('albums_full_info.json', 'r') as f2:
            albums_full_info = json.load(f2)
        new_track_ids = get_new_track_ids_op(clean_albums, albums_full_info)
        new_track_ids.to_csv('new_track_ids.csv', index=False)
        print("NEW TRACK IDS GOT")
    # [END get_new_track_ids]

    # [START extract_new_track_info]
    def extract_new_track_info(**kwargs):
        ti = kwargs['ti']
        new_track_ids = pd.read_csv('new_track_ids.csv')
        new_track_ids['artists_id'] = new_track_ids['artists_id'].apply(eval)
        new_tracks, extract_date_track = extract_new_track_info_op(new_track_ids)
        with open('new_tracks.json', 'w', encoding='utf-8') as f:
            json.dump(new_tracks, f, ensure_ascii=False, indent=4)
        ti.xcom_push('extract_date_track', str(extract_date_track))
        print("NEW TRACK INFO EXTRACTED")
    # [END extract_new_track_info]

    # [START clean_new_track_info]
    def clean_new_track_info(**kwargs):
        ti = kwargs['ti']
        extract_date_track = ti.xcom_pull(task_ids='extract_new_track_info', key='extract_date_track')
        extract_date_track = parser.parse(extract_date_track)
        new_track_ids = pd.read_csv('new_track_ids.csv')
        new_track_ids['artists_id'].apply(eval)
        with open('new_tracks.json', 'r') as f:
            new_tracks = json.load(f)
        clean_new_tracks = clean_new_track_info_op(new_tracks, new_track_ids, extract_date_track)
        clean_new_tracks.to_csv('clean_new_tracks.csv', index=False)
        print("NEW TRACK INFO CLEANED")
    # [END clean_new_track_info]

    # [START extract_audio_features]
    def extract_audio_features(**kwargs):
        ti = kwargs['ti']
        new_track_ids = pd.read_csv('new_track_ids.csv')
        new_track_ids['artists_id'] = new_track_ids['artists_id'].apply(eval)
        audio_features = extract_audio_features_op(new_track_ids)
        with open('audio_features.json', 'w', encoding='utf-8') as f:
            json.dump(audio_features, f, ensure_ascii=False, indent=4)
        print("AUDIO FEATURES EXTRACTED")
    # [END extract_audio_features]

    # [START clean_audio_features]
    def clean_audio_features(**kwargs):
        ti = kwargs['ti']
        with open('audio_features.json', 'r') as f:
            audio_features = json.load(f)
        clean_audio_features = clean_audio_features_op(audio_features)
        clean_audio_features.to_csv('clean_audio_features.csv', index=False)
        print("AUDIO FEATURES CLEANED")
    # [END clean_audio_features]

    # [START extract_artist]
    def extract_artist(**kwargs):
        ti = kwargs['ti']
        with open('clean_albums.json', 'r') as f:
            clean_albums = json.load(f)
        clean_albums = pd.DataFrame(clean_albums)
        clean_albums.reset_index(drop=True, inplace=True)
        clean_albums['release_date'] = clean_albums['release_date'].apply(parser.parse)
        clean_albums['extract_date'] = clean_albums['extract_date'].apply(parser.parse)
        new_artists = extract_artist_op(clean_albums)
        with open('new_artists.json', 'w', encoding='utf-8') as f:
            json.dump(new_artists, f, ensure_ascii=False, indent=4)
        print("ARTIST EXTRACTED")
    # [END extract_artist]

    # [START clean_artist]
    def clean_artist(**kwargs):
        ti = kwargs['ti']
        with open('new_artists.json', 'r') as f:
            new_artists = json.load(f)
        clean_new_artists = clean_artist_op(new_artists)
        clean_new_artists.to_csv('clean_new_artists.csv', index=False)
        print("ARTIST CLEANED")
    # [END clean_artist]

    # [START load_new_albums]
    def load_new_albums(**kwargs):
        ti = kwargs['ti']
        load_new_albums_op()
    # [END load_new_albums]

    # [START load_new_track_info]
    def load_new_track_info(**kwargs):
        ti = kwargs['ti']
        load_new_track_info_op()
    # [END load_new_track_info]
    
    # [START load_audio_features]
    def load_audio_features(**kwargs):
        ti = kwargs['ti']
        # clean_audio_features = pd.read_csv('clean_audio_features.csv',dtype={
        #     "id": str,
        #     "danceability": float,
        #     "energy": float,
        #     "key": int,
        #     "loudness": float,
        #     "mode": int,
        #     "speechiness": float,
        #     "acousticness": float,
        #     "instrumentalness": float,
        #     "liveness": float,
        #     "valence": float,
        #     "tempo": int,
        #     "duration_ms": int,
        #     "time_signature": int,
        # })
        clean_audio_features = pd.read_csv('clean_audio_features.csv')
        load_audio_features_op(clean_audio_features)
        print("AUDIO FEATURES LOADED")
    # [END load_audio_features]

    # [START load_artist_to_history]
    def load_artist_to_history(**kwargs):
        ti = kwargs['ti']
        load_artist_to_history_op()
    # [END load_artist_to_history]

    # [START main_flow]
    Task1 = PythonOperator(
        task_id='extract_new_releases',
        python_callable=extract_new_releases,
    )
    Task1.doc_md = dedent(
        ''''''
    )

    Task2 = PythonOperator(
        task_id='clean_new_albums',
        python_callable=clean_new_albums,
    )
    Task2.doc_md = dedent(
        ''''''
    )

    # Task3 = PythonOperator(
    #     task_id='load_new_albums',
    #     python_callable=load_new_albums,
    # )
    # Task3.doc_md = dedent(
    #     ''''''
    # )
    
    Task4 = PythonOperator(
        task_id='extract_new_albums_full_info',
        python_callable=extract_new_albums_full_info,
        execution_timeout=timedelta(seconds=30),
    )
    Task4.doc_md = dedent(
        ''''''
    )

    Task5 = PythonOperator(
        task_id='extract_artist',
        python_callable=extract_artist,
    )
    Task5.doc_md = dedent(
        ''''''
    )

    Task6 = PythonOperator(
        task_id='get_new_track_ids',
        python_callable=get_new_track_ids,
    )
    Task6.doc_md = dedent(
        ''''''
    )
    
    Task7 = PythonOperator(
        task_id='extract_new_track_info',
        python_callable=extract_new_track_info,
    )
    Task7.doc_md = dedent(
        ''''''
    )

    Task8 = PythonOperator(
        task_id='extract_audio_features',
        python_callable=extract_audio_features,
    )
    Task8.doc_md = dedent(
        ''''''
    )

    Task9 = PythonOperator(
        task_id='clean_new_track_info',
        python_callable=clean_new_track_info,
    )
    Task9.doc_md = dedent(
        ''''''
    )

    # Task10 = PythonOperator(
    #     task_id='load_new_track_info',
    #     python_callable=load_new_track_info,
    # )
    # Task10.doc_md = dedent(
    #     ''''''
    # )

    Task11 = PythonOperator(
        task_id='clean_audio_features',
        python_callable=clean_audio_features,
    )
    Task11.doc_md = dedent(
        ''''''
    )

    Task12 = PythonOperator(
        task_id='load_audio_features',
        python_callable=load_audio_features,
    )
    Task12.doc_md = dedent(
        ''''''
    )

    Task13 = PythonOperator(
        task_id='clean_artist',
        python_callable=clean_artist,
    )
    Task13.doc_md = dedent(
        ''''''
    )

    # Task14 = PythonOperator(
    #     task_id='load_artist_to_history',
    #     python_callable=load_artist_to_history,
    # )
    # Task14.doc_md = dedent(
    #     ''''''
    # )

    '''
    Task1 >> Task2 >> [Task3, Task4, Task5]
    Task4 >> Task6 >> [Task7, Task8]
    Task7 >> Task9 >> Task10
    Task8 >> Task11 >> Task12
    Task5 >> Task13 >> Task14
    '''
    # Without load
    Task1 >> Task2 >> [Task4, Task5]
    Task4 >> Task6 >> [Task7, Task8]
    Task7 >> Task9
    Task8 >> Task11 >> Task12
    Task5 >> Task13
    
    # [END main_flow]