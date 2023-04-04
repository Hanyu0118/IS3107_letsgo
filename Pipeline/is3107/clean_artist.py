import pandas as pd

def clean_artist_op(new_artists):
    id = []
    name = []
    followers = []
    popularity = []

    for i, artist in enumerate(new_artists):
        id.append(artist['id'])
        name.append(artist['name'])
        followers.append(artist['followers']['total'])
        popularity.append(artist['popularity'])

    clean_new_artists = pd.DataFrame({
        'id': id,
        'name': name,
        'followers': followers,
        'popularity': popularity
    })

    clean_new_artists.drop_duplicates(subset=['id'], inplace=True)
    return clean_new_artists