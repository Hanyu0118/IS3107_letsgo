import pandas as pd
import json

def clean_new_albums_op(albums, extract_date_album):
    artists_id = [] # further division
    available_markets = []
    id = []
    name = []
    release_date = []
    total_tracks = []

    for i, album in enumerate(albums):
        available_markets.append(len(album['available_markets']))
        id.append(album['id'])
        name.append(album['name'])
        release_date.append(album['release_date'])
        total_tracks.append(album['total_tracks'])
        
        aids = []
        for i, artist in enumerate(album['artists']):
            aids.append(artist['id'])
        artists_id.append(aids)

    clean_albums = pd.DataFrame({
        'id': id,
        'name': name,
        'total_tracks': total_tracks,
        'available_markets': available_markets,
        'release_date': release_date,
        'artists_id': artists_id
    })
    clean_albums['extract_date'] = extract_date_album
    return clean_albums
