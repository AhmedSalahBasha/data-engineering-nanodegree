# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays 
        (songplay_id SERIAL, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar(50), 
        song_id varchar(100), 
        artist_id varchar(100), 
        session_id int, 
        location varchar(100), 
        user_agent varchar(250),
        PRIMARY KEY (songplay_id));
""")

user_table_create = ("""
    CREATE TABLE users 
        (user_id int, first_name varchar(100), last_name varchar(100), gender varchar(100), level varchar(100),
        PRIMARY KEY (user_id));
""")

song_table_create = ("""
    CREATE TABLE songs 
        (song_id varchar(100), title varchar(100) NOT NULL, artist_id varchar(100), year int, duration numeric NOT NULL,
        PRIMARY KEY (song_id));
""")

artist_table_create = ("""
    CREATE TABLE artists 
        (artist_id varchar(100), name varchar(100) NOT NULL, location varchar(100), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id));
""")

time_table_create = ("""
    CREATE TABLE time
        (start_time timestamp, hour int, day int, week int, month int, year int, weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, 
                            user_id, 
                            level,
                            song_id,
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
    SELECT 
        s.song_id, a.artist_id
    FROM songs s
    INNER JOIN artists a ON (a.artist_id = s.artist_id)
    WHERE s.title = (%s)
    AND a.name = (%s)
    AND s.duration = (%s);
""")


# This one is a little more complicated since information from the songs table, artists table, and original log file are # all needed for the songplays table. Since the log file does not specify an ID for either the song or the artist, you'll # need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title,
# artist name, and song duration time.

# QUERY LISTS

create_table_queries = [time_table_create, 
                        artist_table_create, 
                        user_table_create, 
                        song_table_create, 
                        songplay_table_create]

drop_table_queries = [songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]