import json
import os
import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def lambda_handler(event, context):
    
    #establish connection with spotify server.
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    #data extraction
    playlists = sp.user_playlists('spotify')
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    #establish connection between internal services of aws (lambda and s3).
    client = boto3.client('s3')
    
    #define filename
    fileName = "spotify_raw_data_"+ str(datetime.now()) +".json"
    
    #load spotify data into staging zone (s3 bucket).
    client.put_object(
        Bucket = "spotify-etl-bucket-001",
        Key = "staging_zone/raw_data/"+fileName,
        Body = json.dumps(spotify_data)
    )
