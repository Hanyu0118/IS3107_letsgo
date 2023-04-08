def pop_predict(vars):
    import joblib
    import pandas as pd
    import datetime as dt
    from sklearn.preprocessing import FunctionTransformer

    vars_df = pd.DataFrame([vars])
    vars_df = vars_df.set_axis(['release_date', 'danceability', 'energy', 'key', 'loudness', 'mode',
       'speechiness', 'acousticness', 'instrumentalness', 'liveness',
       'valence', 'tempo', 'duration_ms', 'time_signature', 'explicit',
       'available_markets', 'followers', 'popularity_artist'], axis=1, inplace=False)
    vars_df['release_date']=vars_df['release_date'].map(dt.datetime.toordinal)
    instrumentalness_tranformer = FunctionTransformer(instrumentalness_criteria)
    instrumentalness_tranformer.transform(vars_df)
    speechiness_tranformer = FunctionTransformer(speechiness_criteria)
    speechiness_tranformer.transform(vars_df)
    pop_prepro_from_joblib = joblib.load('Popularity Prediction Data preprocess.sav')
    pop_poly_trans_joblib = joblib.load('Popularity Prediction Poly Transform.sav')
    pop_from_joblib = joblib.load('Popularity Prediction Model.sav')
    processed_vars_df = pd.DataFrame(pop_prepro_from_joblib.transform(vars_df).tolist())
    poly_trans_df = pop_poly_trans_joblib.transform(processed_vars_df)
    return pop_from_joblib.predict(poly_trans_df).clip(0,1)

def instrumentalness_criteria(X):
    X.loc[:,'instrumentalness'] = list(map((lambda x: 1 if x < 0.1 else (3 if x > 0.4 else 2)), X.loc[:,'instrumentalness']))


def speechiness_criteria(X):
    X.loc[:,'speechiness'] = list(map((lambda x: 1 if x < 0.1 else (3 if x > 0.95 else 2)), X.loc[:,'speechiness']))

