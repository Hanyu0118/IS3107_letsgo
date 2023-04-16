from datetime import datetime
from .connect_to_spotify import connect_to_spotify_op

def extract_new_track_info_op(new_track_ids):
    sp = connect_to_spotify_op()
    new_tracks = []
    for i in range(0, len(new_track_ids), 50):
        new_tracks += sp.tracks(new_track_ids.iloc[i:i+50]['track_id'])['tracks']
    extract_date_track = datetime.today()
    return new_tracks, extract_date_track