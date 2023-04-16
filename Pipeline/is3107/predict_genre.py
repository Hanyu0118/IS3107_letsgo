from .connect_to_bigquery import connect_to_bigquery_op
import pickle
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from google.cloud import bigquery
from .config_schema_genre_prediction import config_schema_genre_prediction_op
path=os.getcwd()

def predict_genre_op():
    client = connect_to_bigquery_op()
    
    #Load model
    gb = pickle.load(open('../../Model/Genre Prediction Model.sav','rb'))

    #Load genre table
    table = 'SELECT distinct * FROM snappy-boulder-378707.NewReleases.NewAudioFeatures'
    genre = client.query(table).to_geodataframe()

    dummy_cols = ['key','mode','time_signature']
    genre= pd.get_dummies(genre, columns=dummy_cols)
    genre.drop(['energy', 'loudness'],axis=1, inplace=True)
    
    #Filled in missing features
    features = pd.read_csv(f"../../Model/Genre_classification_features.csv")
    for col in set(features.iloc[:,0].values) - set(genre.iloc[:,1:].columns):
        genre[col] = 0
    
    #Min max transform
    sc = MinMaxScaler(feature_range = (0,100))
    genre_trans = sc.fit_transform(genre.iloc[:,1:])

    #predict
    pred_genre = gb.predict(genre_trans)
    genre['Genre'] = pred_genre
    genre = genre[['id','Genre']]
    
    #delete previous one
    delete = client.query("""
        TRUNCATE TABLE snappy-boulder-378707.Prediction.Genre
    """)
    delete.result()

    #Insert new one
    table_id = 'snappy-boulder-378707.Prediction.Genre'
    schema, job_config = config_schema_genre_prediction_op()
    job = client.load_table_from_dataframe(genre,table_id, schema = schema, job_config=job_config)
    job.result()
    print(f'{client.get_table(table_id).num_rows} is loaded into genre prediction')
