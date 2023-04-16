from .connect_to_bigquery import connect_to_bigquery_op
import pickle
import pandas as pd
import os
import joblib
import datetime as dt
from google.cloud import bigquery
from .config_schema_popularity_prediction import config_schema_popularity_prediction_op
path=os.getcwd()

def predict_genre_op():
    client = connect_to_bigquery_op()
    
    #Load model
    gb = pickle.load(open('../../Model/Genre Prediction Model.sav','rb'))

    #Load genre table
    table = '''SELECT distinct s1.id,
                s1.explicit,
                s1.available_markets,
                artist_id,
                danceability,
                energy,
                key,
                loudness,
                mode,
                speechiness,
                acousticness,
                instrumentalness,
                liveness,
                valence,
                tempo,
                duration_ms,
                time_signature,
                release_date FROM `snappy-boulder-378707.NewReleases.NewTracks`  as s1
                inner join `snappy-boulder-378707.NewReleases.NewAudioFeatures`as s2
                on s1.id = s2.id
                inner join `snappy-boulder-378707.NewReleases.NewAlbums` as s3
                on s1.album_id = s3.id
            '''
    popularity = client.query(table).to_geodataframe()

    table = '''SELECT distinct id, followers, popularity FROM `snappy-boulder-378707.TrackClearInfo.ArtistInfo`'''
    artist = client.query(table).to_geodataframe()

    popularity = popularity.assign(artist_id=popularity.artist_id.str.split(";")).explode('artist_id')
    popularity = pd.merge(popularity, artist, left_on="artist_id",right_on="id", how="left")
    popularity.columns = ['id_track', 'explicit', 'available_markets', 'artist_id', 'danceability',
       'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
       'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms',
       'time_signature', 'release_date', 'artist_id', 'followers', 'popularity_artist']
    popularity.drop_duplicates(inplace=True)
    popularity = popularity.groupby(['id_track', 'explicit', 'available_markets','danceability',
       'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
       'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms',
       'time_signature', 'release_date'], as_index=False).aggregate({'followers':'mean','popularity_artist':'mean'})
    popularity['release_date']=popularity['release_date'].map(dt.datetime.toordinal)
    prediction = popularity[['id_track']]
    popularity.drop(['id_track'],axis=1, inplace=True)

    ##Transform
    ct = joblib.load("../../Model/Popularity Prediction Data Preprocess.joblib")
    popularity = ct.transform(popularity)


    #predict
    lgbm = joblib.load(open('../../Model/Popularity Prediction Model.sav','rb'))
    pred_popularity = gb.predict(popularity)
    prediction['Popularity'] = pred_popularity
    prediction.columns = ['id','Popularity']
    
    #delete previous one
    delete = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.Prediction.Popularity
    """)
    delete.result()

    #Insert new one
    table_id = 'snappy-boulder-378707.Prediction.Popularity'
    schema, job_config = config_schema_popularity_prediction_op()
    job = client.load_table_from_dataframe(prediction,table_id, schema = schema, job_config=job_config)
    job.result()
    print(f'{client.get_table(table_id).num_rows} is loaded into genre prediction')
