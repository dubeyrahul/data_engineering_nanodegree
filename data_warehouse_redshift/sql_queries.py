import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_fact"
user_table_drop = "DROP TABLE IF EXISTS user_dim"
song_table_drop = "DROP TABLE IF EXISTS song_dim"
artist_table_drop = "DROP TABLE IF EXISTS artist_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar NOT NULL,
    firstName varchar,
    gender char(1),
    itemInSession int NOT NULL,
    lastName varchar,
    length float,
    level varchar NOT NULL,
    location varchar,
    method char(3) NOT NULL,
    page varchar NOT NULL,
    registration float,
    sessionId int NOT NULL,
    song varchar,
    status SMALLINT NOT NULL,
    ts integer NOT NULL,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id varchar NOT NULL,
    artist_latitude float,
    artist_location varchar,
    artist_longitude float,
    artist_name varchar,
    duration float NOT NULL,
    num_songs int NOT NULL,
    song_id varchar NOT NULL,
    title varchar NOT NULL,
    year int NOT NULL
)
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM {config.get('S3', 'LOG_DATA')}
CREDENTIALS {config.get('IAM_ROLE', 'ARN')}
JSON {config.get('S3','LOG_JSONPATH')}
""")

staging_songs_copy = ("""
COPY staging_songs
FROM {config.get('S3', 'SONG_DATA')}
CREDENTIALS {config.get('IAM_ROLE', 'ARN')}
JSON 'auto'
""")

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
