from datetime import date
from .connect_to_spotify import connect_to_spotify_op

def extract_new_releases_op():
    # credentials = json.load(open('authorization.json'))
    # client_id = credentials['client_id']
    # client_secret = credentials['client_secret']
    sp = connect_to_spotify_op()
    albums = sp.new_releases()['albums']['items']
    extract_date_album = date.today()
    return albums, extract_date_album