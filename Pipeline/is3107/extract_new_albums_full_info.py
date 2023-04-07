import pandas as pd
from dateutil import parser
from .connect_to_spotify import connect_to_spotify_op

def extract_new_albums_full_info_op(clean_albums):
    sp = connect_to_spotify_op()
    ids = clean_albums['id'].tolist()
    albums_full_info = []
    for i in range(0, len(ids), 20):
        albums_full_info += sp.albums(ids[i:i+20])['albums']
    return albums_full_info
