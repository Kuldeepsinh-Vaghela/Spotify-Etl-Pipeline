import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager =  SpotifyClientCredentials(client_id = client_id ,client_secret = client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    playlist_link = "https://open.spotify.com/artist/6eUKZXaKkcviH0Ku9w2n3V"
    artist_id = playlist_link.split('/')[-1]

    artist_track_data = sp.artist_top_tracks(artist_id)

    client = boto3.client('s3')

    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    client.put_object(
        Bucket= 'spotify-etl-project-kuldeep',
        Key= 'raw-data/to-processed/' + filename ,
        Body= json.dumps(artist_track_data)
    )

    glue = boto3.client('glue')

    gluejobname = "spotify-etl-transformation-job"

    try:
        runid = glue.start_job_run(JobName = gluejobname)
        status = glue.get_job_run(JobName = gluejobname, RunId = runid['JobRunId'])
        print(status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
        print('Error starting job run')
        raise e

   
