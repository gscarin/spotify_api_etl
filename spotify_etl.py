##Spotify Pipeline usiing Airflow and Spotify API
## Save each music lintened in a day
## Every day refreshes data

from os import execvpe
import requests
import sqlalchemy
from key import AUTH
import datetime
import json
import pandas as pd
from sqlalchemy.orm import sessionmaker
import sqlite3

def check_response(r) -> bool:
    if r.status_code == 401:
        raise Exception ('Check your api key')
    return False



def check_dataframe( df: pd.DataFrame) -> bool:
    ##check if dataframe is empty == No songs played at that day
    if df.empty:
        print('No song downloaded. Closing...')
        return False


    # check primary key == played at ( timestamp)

    if pd.Series(df["played_at"]).is_unique:
        pass  
    else:
        raise Exception('Primary Key Violation')

    #checking nulls
    if df.isnull().values.any():
        raise Exception ('Null values')

    #checking all time stamps are yesterday
'''
    yesterday_2 = datetime.datetime.now() - datetime.timedelta(days = DAYS)
    yesterday_2 = yesterday_2.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    timestamps = df['time'].tolist()

    for time in timestamps:
    
        if datetime.datetime.strptime(time, "%Y-%m-%d") != yesterday_2:
            raise Exception ("At least one of the songs wasnt played within the last 24 hours")
    return True
'''

def run_spotify_etl():
    ##local address
    DATABASE_LOCATION =   'sqlite:///my_played_tracks.sqlite'
    USER_ID = 'your-id'
    TOKEN = AUTH
    DAYS = 1

    headers = {
    'Accept':'application/json',
    'Content-Type':'application/json',
    'Authorization' : 'Bearer {token}'.format(token = TOKEN)
    
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = DAYS)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time = yesterday_unix),headers= headers)

    if check_response(r):
        print ('You need to refresh your Api-Key')
    data = r.json()

    song_names = []
    artist_names = []
    played_list = []
    time = []

## looping to append info to each song lintened
    for song in data['items']:
        song_names.append(song["track"]['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_list.append(song['played_at'])
        time.append(song['played_at'][0:10])

##dict do input in dataframe
    song_dict = {
        "song_name" : song_names,
        'artist_name':artist_names,
        'played_at':played_list,
        'time':time
    }

## Data Frame

    df_songs = pd.DataFrame(song_dict)
    


    if check_dataframe(df_songs):
        print("Data valid, proceeding to loading stage...")

    ## Load data into Sqlite table

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('played_tracks.sqlite')
    cursor = conn.cursor()


    create_table_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        time VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(create_table_query)

    print('Connection established...')

    try:
        df_songs.to_sql('my_played_tracks', engine, index = False, if_exists='append')
        print('Inserted songs into database')
    except:
        print('Data already exists in database')

    conn.close()
    print('Closing connection....')

