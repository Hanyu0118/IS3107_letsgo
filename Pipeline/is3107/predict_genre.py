from is3107.connect_to_bigquery import connect_to_bigquery_op
import pandas as pd
import numpy as np
import os
import joblib
from google.cloud import bigquery
from is3107.config_schema_genre_prediction import config_schema_genre_prediction_op
path=os.getcwd()

def predict_genre_op():
    client = connect_to_bigquery_op()
    
    #Load model
    xgb = joblib.load(open(f'{path}/airflow/dags/Model/Genre Prediction Model.sav','rb'))

    #Load genre table
    table = 'SELECT distinct * FROM snappy-boulder-378707.NewReleases.NewAudioFeatures'
    genre = client.query(table).to_dataframe()

    dummy_cols = ['key','mode','time_signature']
    genre= pd.get_dummies(genre, columns=dummy_cols)
    genre.drop(['energy', 'loudness'],axis=1, inplace=True)
    
    #Filled in missing features
    features = pd.read_csv(f'{path}/airflow/dags/Model/Genre_classification_features.csv')
    for col in set(features.iloc[:,0].values) - set(genre.iloc[:,1:].columns):
        genre[col] = 0
    
    #Min max transform
    sc = joblib.load(f'{path}/airflow/dags/Model/Genre Prediction Data Preprocess.joblib')
    genre = genre[['id','danceability', 'speechiness', 'acousticness', 'instrumentalness',
    'liveness', 'valence', 'tempo', 'duration_ms', 'key_0', 'key_1',
    'key_2', 'key_3', 'key_4', 'key_5', 'key_6', 'key_7', 'key_8',
    'key_9', 'key_10', 'key_11', 'mode_0', 'mode_1',
    'time_signature_0', 'time_signature_1', 'time_signature_3',
    'time_signature_4', 'time_signature_5']]
    genre_trans = sc.transform(genre.iloc[:,1:])

    #predict
    pred_genre = xgb.predict(genre_trans)
    pred_genre = pd.DataFrame(pred_genre.toarray())
    genre = pd.concat([genre[['id']], pred_genre], axis=1)
    
    #Set columns
    labels = pd.read_csv(f'{path}/airflow/dags/Model/Genre Prediction Label.csv')
    genre.columns = ['id'] + labels['Label'].to_list()

    #Melt
    genre_unpivot = genre.melt(
    id_vars=['id'], var_name='Genre', value_name='binary')
    genre_unpivot = genre_unpivot[genre_unpivot["binary"] == 1]
    genre_unpivot.drop(['binary'], axis=1, inplace=True)
    
    #delete previous one
    delete = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.NewReleases.GenrePrediction
    """)
    delete.result()

    #Insert new one
    table_id = 'snappy-boulder-378707.NewReleases.GenrePrediction'
    schema, job_config = config_schema_genre_prediction_op()
    job = client.load_table_from_dataframe(genre_unpivot,table_id,  job_config=job_config)
    job.result()
    print(f'{client.get_table(table_id).num_rows} is loaded into genre prediction')
