import pandas as pd
from dateutil import parser
from .connect_to_spotify import connect_to_spotify_op

def extract_new_albums_full_info_op(clean_albums):
    sp = connect_to_spotify_op()
    clean_albums = pd.DataFrame(clean_albums)
    clean_albums['release_date'] = clean_albums['release_date'].apply(parser.parse)
    clean_albums['extract_date'] = clean_albums['extract_date'].apply(parser.parse)
    albums_full_info = sp.albums(clean_albums['id'].tolist())['albums']
    return albums_full_info
