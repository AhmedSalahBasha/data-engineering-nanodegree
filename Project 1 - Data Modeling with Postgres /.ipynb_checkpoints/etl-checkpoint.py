import os
import glob
import psycopg2
import pandas as pd
import itertools
from sql_queries import *

    
def process_song_file(cur, filepath):
    """
    This function loads data files related to song_data from data directory.
    Also, it filters-out unimportant columns and split the loaded dataframe 
    into song_data and artist_data.
    Finally, it inserts both dataframes into their corresponding tables in the database.
    param cur: database cursor connection
    param filepath: the data directory path
    """
    # open song file
    df = pd.read_json(filepath, orient='records', typ='series')

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function loads data files related to log_data from data directory.
    It filters the data by page = 'NextSong'. 
    It extracts Time and Users data and saves them to separate dataframes.
    It generates the fact table (Songplays) out of the original data combining with Artists and Songs tables.
    Finally, it inserts the three dataframes into their corresponding tables in the database (Time, Users, Songplays).
    param cur: database cursor connection
    param filepath: the data directory path
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].drop_duplicates().dropna()

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = time_data = [list(df.ts), 
                             list(df.ts.dt.hour), 
                             list(df.ts.dt.day), 
                             list(df.ts.dt.week), 
                             list(df.ts.dt.month), 
                             list(df.ts.dt.year), 
                             list(df.ts.dt.weekday)]
    
    # -- source: https://stackoverflow.com/questions/6473679/transpose-list-of-lists
    time_data = list(map(list, itertools.zip_longest(*time_data, fillvalue=None)))
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        params = (row.song, row.artist, row.length)
        cur.execute(song_select, params)
        results = cur.fetchone()
        if results:
            songid, artistid = results
            # insert songplay record
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
            print("The following data {0} \n is inserted successfully in table Songplays".format(songplay_data))
        else:
            songid, artistid = None, None


def process_data(cur, conn, filepath, func):
    """
    This function implements the data loading from the data directory.
    Then, it sends each loaded file to the corresponsing function for further processing
    param cur: database cursor connection
    param conn: database connection object
    param filepath: the data directory path
    func: a function the process the data file
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
    """
    This is the main function which is the entry point to the pipeline.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()