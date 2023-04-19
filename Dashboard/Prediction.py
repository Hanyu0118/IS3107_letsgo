import joblib
import pandas as pd
import datetime as dt
from sklearn.preprocessing import FunctionTransformer
import os
path = os.getcwd()

def pop_predict(vars):
    vars_df = pd.DataFrame([vars])
    vars_df = vars_df.set_axis(['release_date', 'danceability', 'energy', 'key', 'loudness', 'mode',
       'speechiness', 'acousticness', 'instrumentalness', 'liveness',
       'valence', 'tempo', 'duration_ms', 'time_signature', 'explicit',
       'available_markets', 'followers', 'popularity_artist'], axis=1, inplace=False)
    vars_df['release_date']=100
    vars_df[['key','mode','time_signature','explicit']] = vars_df[['key','mode','time_signature','explicit']].astype('Int64')  
    print(vars_df.dtypes)
    print(path)
    pop_prepro_from_joblib = joblib.load(f'{path}/Model/Popularity Prediction Data Preprocess.joblib')
    pop_from_joblib = joblib.load(f'{path}/Model/Popularity Prediction Model.sav')
    processed_vars_df = pd.DataFrame(pop_prepro_from_joblib.transform(vars_df))
    return pop_from_joblib.predict(processed_vars_df)

def genre_predict(vars):
   #Load model
   xgb_genre = joblib.load(open(f'{path}/Model/Genre Prediction Model.sav','rb'))

   genre = pd.DataFrame([vars], columns= ['danceability','energy','key','loudness','mode',
                                           'speechiness', 'acousticness', 'instrumentalness',
                                          'liveness', 'valence', 'tempo', 'duration_ms','time_signature']) 
   dummy_cols = ['key','mode','time_signature']
   genre= pd.get_dummies(genre, columns=dummy_cols)
   genre.drop(['energy', 'loudness'],axis=1, inplace=True)

   #Filled in missing features
   features = pd.read_csv(f'{path}/Model/Genre_classification_features.csv')
   for col in set(features.iloc[:,0].values) - set(genre.columns):
      genre[col] = 0

   #Min max transform
   sc = joblib.load(f'{path}/Model/Genre Prediction Data Preprocess.joblib')
   genre = genre[['danceability', 'speechiness', 'acousticness', 'instrumentalness',
   'liveness', 'valence', 'tempo', 'duration_ms', 'key_0', 'key_1',
   'key_2', 'key_3', 'key_4', 'key_5', 'key_6', 'key_7', 'key_8',
   'key_9', 'key_10', 'key_11', 'mode_0', 'mode_1',
   'time_signature_0', 'time_signature_1', 'time_signature_3',
   'time_signature_4', 'time_signature_5']]
   genre_trans = sc.transform(genre)

   #predict
   labels = pd.read_csv(f'{path}/Model/Genre Prediction Label.csv')
   pred_genre = xgb_genre.predict(genre_trans)
   pred_genre = pd.DataFrame(pred_genre.toarray(), columns = labels['Label'].to_list())
   pred_genre['id'] = 'Prediction'

   #Melt
   genre_unpivot = pred_genre.melt(
   id_vars=['id'], var_name='Genre', value_name='binary')
   genre_unpivot = genre_unpivot[genre_unpivot["binary"] == 1]
   genre_unpivot.drop(['binary'], axis=1, inplace=True)
   print(genre_unpivot)
   return ','.join(genre_unpivot.Genre.values.tolist())