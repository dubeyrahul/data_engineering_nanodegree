import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.DataFrame([pd.read_json(filepath,  typ='series', convert_dates=False)])

    # insert song record
    song_columns_to_select = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_columns_to_select].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns_to_select = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    artist_data = df[artist_columns_to_select].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def get_time_data_from_df(df, timestamp_col):
    datetime_col= 'start_time'
    df[datetime_col] =  pd.to_datetime(df[timestamp_col], unit='ms')
    time_units = ['hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data_series = [df[datetime_col]]
    for time_unit in time_units:
        time_data_series.append(getattr(df[datetime_col].dt,time_unit).rename(time_unit))
    return pd.concat(time_data_series, axis=1)

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = get_time_data_from_df(df, 'ts') 
    
    # insert time data records
    time_data = t.values.tolist()
    column_labels = t.columns.tolist()
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName','gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # construct a songplay_id from user_id and timestamp as this combination will be unique (assuming user can play from only one platform at a time
        songplay_id = 'u'+str(row.userId)+'_t'+str(row.ts)
        songplay_data = (songplay_id, row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
