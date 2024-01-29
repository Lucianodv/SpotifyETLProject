from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta, timezone
import datetime
import requests
import base64
import json
from requests import post
import sqlalchemy
import sqlite3
import spotipy
import pandas as pd




#jobs_scheduler
def run_spotify_etl():
    
    #CONSTANTES
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    USER_ID = "LucianoVenialgo"
    CLIENT_ID = "YOUR_CLIENT_ID"
    CLIENT_SECRET = "YOUR_CLIENT_SECRET"
    SPOTIPY_REDIRECT_URI = "https://google.com"
    SCOPE = 'user-read-recently-played'


    #Validation data
    def Check_valid_data(df: pd.DataFrame) -> bool:
        if df.empty:
            print('No songs downloades. Finish execution')
            return False
        else:
            print('Songs download successful.')


        #Check for nulls
        if df.isnull().values.any():
            raise Exception('Null detected: Finish Execution')




    #date format unix
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp())
    yesterday_date = yesterday.strftime("%Y-%m-%d")

    #conection with spotify
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id = CLIENT_ID,
                                               client_secret = CLIENT_SECRET,
                                               redirect_uri = SPOTIPY_REDIRECT_URI,
                                               scope= SCOPE
                                               ))

    #Get Token to access
    tokens = sp.auth_manager.get_access_token()

    #Get Data information songs spotify
    data = sp.current_user_recently_played(limit=50, after=yesterday_unix_timestamp)
    #print(data)

    #Variables
    id= []
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    popularity = []
    album_name = []

    for song in data["items"]:
        # Check if the song was played yesterday
        played_at = song['played_at']
        played_at_date = played_at[0:10]

        if played_at_date == yesterday_date:
            id.append(song['track']['id'])
            song_names.append(song['track']['name'])
            artist_names.append(song['track']['album']['artists'][0]['name'])
            played_at_list.append(song['played_at'])
            timestamps.append(song['played_at'][0:10])
            popularity.append(song['track']['popularity'])
            album_name.append(song['track']['album']['name'])


    song_dict = {
    
        "song_name" : song_names,
        "artist_names" : artist_names,
        "played_at_list" : played_at_list,
        "timestamps" : timestamps,
        "popularity": popularity,
        'id_song' : id,
        'album_name': album_name
    }

    #Table format
    song_df = pd.DataFrame(song_dict, columns = ["song_name","artist_names",'album_name',"played_at_list","timestamps","popularity","id_song"])


    #check dataframe with data
    #print(song_df)

    #Validate to df
    if Check_valid_data(song_df):
        print('Data Valid, proceed to Load')


    #Load Data 
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    #Create conection
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query  = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_names VARCHAR(200),
        album_name VARCHAR(200),
        played_at_list VARCHAR(200),
        timestamps VARCHAR(200),
        popularity VARCHAR(200),
        id_song VARCHAR(200),
        PRIMARY KEY (played_at_list)
        )
    """
    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')

    conn.close()
    print('Close database successfull')

      
run_spotify_etl()

