import configparser

# Load config to read S3 paths
config = configparser.ConfigParser()
config.read('dwh.cfg')

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
# Drop table queries
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_fact"
user_table_drop = "DROP TABLE IF EXISTS user_dim"
song_table_drop = "DROP TABLE IF EXISTS song_dim"
artist_table_drop = "DROP TABLE IF EXISTS artist_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# Create staging tables that will load data from S3
# Create staging_events table to store events log
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT NOT NULL,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method CHAR(3) NOT NULL,
    page VARCHAR NOT NULL,
    registration FLOAT,
    sessionId INT NOT NULL,
    song VARCHAR,
    status SMALLINT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId VARCHAR
)
""")

# Create staging_songs table to store song data
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_location VARCHAR,
    artist_longitude FLOAT,
    artist_name VARCHAR,
    duration FLOAT NOT NULL,
    num_songs INT NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    year VARCHAR NOT NULL
)
""")

# Create fact and dimension tables now
# Create songplay_fact as our fact table
# we select songplay_id as distkey because it is a serial
# and will distribute rows evenly
# we select start_time as sortkey so that time-based where clauses are fast
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_fact (
    songplay_id INT IDENTITY(0, 1) distkey,
    start_time TIMESTAMP NOT NULL sortkey,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
)
""")

# Create user_dim as our dimension table containing user information
# we select user_id as sortkey and distribute this table to all slices
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_dim (
    user_id INT NOT NULL sortkey,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
)
diststyle all
""")

# Create song_dim as our song dimension table containing song information
# we select song_id as our sortkey and distkey because it might be too big
# to fit in on a single slice
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_dim (
    song_id VARCHAR NOT NULL sortkey distkey,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year SMALLINT NOT NULL,
    duration FLOAT NOT NULL
)
""")

# Create artist_dim as our artist dimension table containing artist information
# we select artist_id as our sortkey and distribute this table to all slices
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_dim (
    artist_id VARCHAR NOT NULL sortkey,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude REAL,
    longitude REAL
)
diststyle all
""")

# Create time_dim as our time dimension table containing datetime components
# we select start_time as sortkey and distribute this table to all slices
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_dim (
    start_time TIMESTAMP NOT NULL sortkey,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
)
diststyle all
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM {config.get('S3', 'LOG_DATA')}
CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
JSON {config.get('S3','LOG_JSONPATH')}
REGION 'us-west-2'
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM {config.get('S3', 'SONG_DATA')}
CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
JSON 'auto'
REGION 'us-west-2'
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_fact (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT
        timestamp 'epoch' + staging_events.ts/1000 * interval '1 second' AS start_time,
        CAST(staging_events.userId AS INT) AS user_id,
        staging_events.level AS level,
        staging_songs.song_id AS song_id,
        staging_songs.artist_id AS artist_id,
        staging_events.sessionId AS session_id,
        staging_events.location AS location,
        staging_events.userAgent AS user_agent
    FROM
        staging_events
        JOIN
        staging_songs
        ON
        (
            staging_events.artist = staging_songs.artist_name
            AND
            staging_events.song = staging_songs.title
        )
)
""")

user_table_insert = ("""
INSERT INTO user_dim
(
    SELECT
        DISTINCT(CAST(userId AS INT)) AS user_id,
        firstName AS first_name,
        lastName as last_name,
        gender AS gender,
        level AS level
    FROM
        staging_events
    WHERE page='NextSong'
)
"""
)

song_table_insert = ("""
INSERT INTO song_dim
(
    SELECT
        DISTINCT(staging_songs.song_id) AS song_id,
        staging_songs.title AS title,
        staging_songs.artist_id AS artist_id,
        staging_songs.year AS year,
        staging_songs.duration AS duration
    FROM
        staging_songs
)
"""
)

artist_table_insert = ("""
INSERT INTO artist_dim
(
    SELECT
        DISTINCT(staging_songs.artist_id) AS artist_id,
        staging_songs.artist_name AS artist,
        staging_songs.artist_location AS location,
        staging_songs.artist_latitude AS latitude,
        staging_songs.artist_longitude AS longitude
    FROM
        staging_songs
)
"""
)

time_table_insert = ("""
INSERT INTO time_dim
(
    SELECT
        start_time AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM
        (
          SELECT timestamp 'epoch' + staging_events.ts/1000 * interval '1 second' AS start_time
          FROM staging_events
        )
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
