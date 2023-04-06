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
    pop_prepro_from_joblib = joblib.load('Popularity Prediction Data preprocess.sav')
    pop_from_joblib = joblib.load('Popularity Prediction Model.sav')
    processed_vars_df = pd.DataFrame(pop_prepro_from_joblib.transform(vars_df).tolist())
    instrumentalness_tranformer = FunctionTransformer(instrumentalness_criteria)
    instrumentalness_tranformer.transform(processed_vars_df)
    speechiness_tranformer = FunctionTransformer(speechiness_criteria)
    speechiness_tranformer.transform(processed_vars_df)
    return pop_from_joblib.predict(processed_vars_df)

def instrumentalness_criteria(X):
    X.iloc[:,30] = list(map((lambda x: 1 if x < 0.1 else (3 if x > 0.4 else 2)), X.iloc[:,30]))


def speechiness_criteria(X):
    X.iloc[:,28] = list(map((lambda x: 1 if x < 0.1 else (3 if x > 0.95 else 2)), X.iloc[:,28]))

