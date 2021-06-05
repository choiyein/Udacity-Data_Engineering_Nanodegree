import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json

def process_song_file(cur, filepath):
    """
        Description: This function is responsible for 
            - opening song files in JSON format,
            - extracting values of the selected columns,
            - inserting those values into song and artist tables in the sparkifydb.

        Arguments:
            cur: the cursor object
            filepath: song data file path
        
        Returns:
            None
    """
    # open song file
    df = json.loads(open(filepath, "r").read())
    df = pd.Series(df).to_frame().transpose()

    # insert song record
    song_data = df.loc[[0], ('song_id', 'title', 'artist_id', 'year', 'duration')].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df.loc[[0], ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Description: This function is responsible for 
            - creating a data frame,
            - opening a log file in JSON format, 
            - inserting values in each log in a log file into the dataframe
            - filtering and transforming the values 
            - inserting transformed values into time, user, and songplays tables in the sparkifydb.

        Arguments:
            cur: the cursor object
            filepath: song data file path
        
        Returns:
            None
    """
    # create data frame
    df = pd.DataFrame(columns=["artist", "auth","firstName","gender",
                               "itemInSession","lastName","length","level",
                               "location","method","page","registration",
                               "sessionId","song","status","ts","userAgent",
                               "userId"])

    # open log file
    for line in open(filepath, "r"):
        dict = json.loads(line)
        df = df.append(dict, ignore_index=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    df['ts'] = t

    # insert time data records
    hour, day, week, month, year, weekday = t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.dayofweek
    time_data = (t, hour, day, week, month, year, weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({k:v for k,v in zip(column_labels, time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ('userId', 'firstName', 'lastName', 'gender', 'level')].drop_duplicates()

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Description: This function is responsible for 
            - listing the files in a directory
            - executing the ingest process for each file according to the function that 
              performs the transformation to save it to the database.

        Arguments:
            cur: cursor object
            conn: connection to the database
            filepath: log data or song data file path
            func: function that transforms the data and inserts it into the database

        Returns:
            None
    """
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
