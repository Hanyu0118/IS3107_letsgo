import pandas as pd

def get_new_track_ids_op(clean_albums, albums_full_info):
    track_id = []
    album_id = []
    artists_id = []
    for i, album in enumerate(albums_full_info):
        for j, track in enumerate(album['tracks']['items']):
            track_id.append(track['id'])
            album_id.append(album['id'])
            artists_id.append(clean_albums.loc[i,'artists_id'])

    new_track_ids = pd.DataFrame({
        'track_id': track_id,
        'album_id': album_id,
        'artists_id': artists_id
    })
    return new_track_ids