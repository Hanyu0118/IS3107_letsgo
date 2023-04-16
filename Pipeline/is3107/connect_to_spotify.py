import spotipy
# from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
def connect_to_spotify_op():
    client_id = "70b6029251944c06b7f4b7b9339a8f86"
    client_secret = "9e8e1bbd325a4b908cccdd077f988320"
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp