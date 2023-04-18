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
    vars_df[['key','mode','time_signature','explicit']] = vars_df[['key','mode','time_signature','explicit']].astype('Int64')  
    print(vars_df.dtypes)
    pop_prepro_from_joblib = joblib.load('Popularity Prediction Data Preprocess.joblib')
    pop_from_joblib = joblib.load('Popularity Prediction Model.sav')
    processed_vars_df = pd.DataFrame(pop_prepro_from_joblib.transform(vars_df))
    return pop_from_joblib.predict(processed_vars_df)