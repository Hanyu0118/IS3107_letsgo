from .connect_to_spotify import connect_to_spotify_op

def extract_audio_features_op(new_track_ids):
    sp = connect_to_spotify_op()
    audio_features = []
    for i in range(0, len(new_track_ids), 100):
        audio_features += sp.audio_features(new_track_ids.iloc[i:i+100]['track_id'])
    return audio_features