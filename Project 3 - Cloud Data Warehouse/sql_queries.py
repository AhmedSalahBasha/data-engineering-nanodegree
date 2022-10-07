import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE stg_events
        (artist VARCHAR(500),
        auth VARCHAR(100),
        firstName VARCHAR(100),
        gender VARCHAR(50),
        itemInSession INT,
        lastName VARCHAR(100),
        length DECIMAL,
        level VARCHAR(50),
        location VARCHAR(500),
        method VARCHAR(100),
        page VARCHAR(100),
        registration TIMESTAMP,
        sessionId INT,
        song VARCHAR(500),
        status INT,
        ts TIMESTAMP,
        userAgent VARCHAR(500),
        userId INT);
""")

staging_songs_table_create = ("""
    CREATE TABLE stg_songs
        (artist_id VARCHAR(100),
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_name VARCHAR(300),
        duration DECIMAL,
        num_songs INT,
        song_id VARCHAR(100),
        title VARCHAR(500),
        year INT);
""")

# ===================================================

songplay_table_create = ("""
    CREATE TABLE songplays 
        (songplay_id INT IDENTITY(0,1) PRIMARY KEY DISTKEY, 
        start_time TIMESTAMP NOT NULL SORTKEY, 
        user_id INT NOT NULL, 
        level VARCHAR(100), 
        song_id VARCHAR(100) NOT NULL, 
        artist_id VARCHAR(100) NOT NULL, 
        session_id INT NOT NULL, 
        location VARCHAR(200), 
        user_agent VARCHAR(500));
""")

user_table_create = ("""
    CREATE TABLE users 
        (user_id INT PRIMARY KEY, 
        first_name VARCHAR(200) SORTKEY, 
        last_name VARCHAR(200), 
        gender VARCHAR(100), 
        level VARCHAR(100))
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE songs 
        (song_id VARCHAR(200) PRIMARY KEY, 
        title VARCHAR(500) NOT NULL SORTKEY, 
        artist_id VARCHAR(100) NOT NULL, 
        year INT, 
        duration DECIMAL NOT NULL)
    DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE artists 
        (artist_id VARCHAR(100) PRIMARY KEY, 
        name VARCHAR(200) NOT NULL SORTKEY, 
        location VARCHAR(500), 
        latitude DOUBLE PRECISION, 
        longitude DOUBLE PRECISION)
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time
        (start_time TIMESTAMP PRIMARY KEY SORTKEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT)
    DISTSTYLE ALL;
""")


# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from '{0}'
    credentials 'aws_iam_role={1}'
    json {2}
    region '{3}'
    timeformat 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA_PATH'), 
            config.get('IAM_ROLE', 'ROLE_ARN'), 
            config.get('S3', 'LOG_JSONPATH'),
            config.get('S3', 'REGION'))

staging_songs_copy = ("""
    copy stg_songs from '{0}'
    credentials 'aws_iam_role={1}'
    region '{2}'
    format as json 'auto';
""").format(config.get('S3', 'SONG_DATA_PATH'), 
            config.get('IAM_ROLE', 'ROLE_ARN'), 
            config.get('S3', 'REGION'))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM stg_events e
    JOIN stg_songs s ON (e.artist = s.artist_name)
    WHERE e.ts IS NOT NULL
    AND e.userId IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level
    FROM stg_events e
    WHERE e.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title , artist_id, year, duration)
    SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
    FROM stg_songs s
    WHERE s.song_id IS NOT NULL
    AND s.title IS NOT NULL
    AND s.duration IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT s.artist_id, s.artist_name, e.location, s.artist_latitude, s.artist_longitude
    FROM stg_songs s
    JOIN stg_events e ON (e.artist = s.artist_name)
    WHERE s.artist_id IS NOT NULL
    AND s.artist_name IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        e.ts, 
        EXTRACT(HOUR FROM e.ts) AS hour, 
        EXTRACT(DAY FROM e.ts) AS day,
        EXTRACT(WEEK FROM e.ts) AS week,
        EXTRACT(MONTH FROM e.ts) AS month,
        EXTRACT(YEAR FROM e.ts) AS year,
        EXTRACT(WEEKDAY FROM e.ts) AS weekday
    FROM stg_events e
    WHERE e.ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
