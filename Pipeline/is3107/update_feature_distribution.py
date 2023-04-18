from is3107.connect_to_bigquery import connect_to_bigquery_op
import pandas as pd
import numpy as np
import os
from datetime import datetime
from google.cloud import bigquery
path=os.getcwd()

def update_feature_distribution_op():
    client = connect_to_bigquery_op()

    nowyear = datetime.now().year
    begin_year = str(nowyear) + '-01-01'
    begin_year = '\'' + begin_year +'\''

    extraction = '''SELECT t3.danceability, t3.energy,t3.key, t3.loudness, t3.mode, t3.speechiness, t3.acousticness, t3.instrumentalness, t3.liveness, t3.valence, t3.tempo, t3.duration_ms, t3.time_signature
                FROM snappy-boulder-378707.History.Tracks as t1
                inner join snappy-boulder-378707.History.Albums as t2
                on t1.album_id = t2.id 
                inner join snappy-boulder-378707.History.AudioFeatures as t3
                on t1.id = t3.id
                where t2.release_date >= ''' + begin_year
    update = client.query(extraction).to_dataframe()
    
    #Calculate
    result = list(update.apply(np.mean, axis=0))
    result = [nowyear] + result + [len(update)]

    #query
    q = ','.join(str(value) for value in result)
    q = '(' + q + ')'
    query = '''INSERT INTO snappy-boulder-378707.History.FeatureDistribution (year, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness,valence, tempo, duration_ms, time_signature, total_tracks)
       VALUES''' + q
    
    #delete original
    delete = client.query('''DELETE FROM snappy-boulder-378707.History.FeatureDistribution where year = 2023''')
    delete.result()

    #insert new
    update = client.query(query)
    update.result()