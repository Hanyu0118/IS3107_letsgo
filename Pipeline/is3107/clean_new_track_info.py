import pandas as pd

def clean_new_track_info_op(new_tracks, new_track_ids, extract_date_track):
    id = []
    name = []
    explicit = []
    available_markets = []
    popularity = []
    album_id = []

    for i, track in enumerate(new_tracks):
        id.append(track['id'])
        name.append(track['name'])
        available_markets.append(len(track['available_markets']))
        explicit.append((1 if track['explicit'] else 0))
        popularity.append(track['popularity'])
        album_id.append(new_track_ids.loc[i,'album_id'])

    clean_new_tracks = pd.DataFrame({
        'id': id,
        'name': name,
        'explicit': explicit,
        'available_markets': available_markets,
        'popularity': popularity,
        'album_id': album_id
    })
    clean_new_tracks['artist_id'] = new_track_ids['artists_id']
    clean_new_tracks['artist_id'] = clean_new_tracks['artist_id'].apply(lambda x: ";".join(x))
    clean_new_tracks['extract_date'] = extract_date_track
    return clean_new_tracks