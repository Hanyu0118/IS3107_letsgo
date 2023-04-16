import pandas as pd

def clean_audio_features_op(audio_features):
    clean_audio_features = pd.DataFrame(audio_features)
    clean_audio_features.reset_index(drop=True, inplace=True)
    clean_audio_features.drop(['type', 'uri', 'track_href', 'analysis_url'], axis=1, inplace=True)
    clean_audio_features = clean_audio_features[['id','danceability','energy','key','loudness','mode','speechiness','acousticness','instrumentalness','liveness','valence','tempo','duration_ms','time_signature']]
    return clean_audio_features