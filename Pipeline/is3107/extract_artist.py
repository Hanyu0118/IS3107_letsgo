from .connect_to_spotify import connect_to_spotify_op

def extract_artist_op(clean_albums):
    sp = connect_to_spotify_op()
    artists = []
    for i in range(len(clean_albums)):
        artists += clean_albums.loc[i,'artists_id']
    
    new_artists = []
    for i in range(0, len(artists), 50):
        new_artists += sp.artists(artists[i:i+50])['artists']
    return new_artists