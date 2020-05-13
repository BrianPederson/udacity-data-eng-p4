# create_tables_helper.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/20/2020
# PURPOSE: Script encapsulating schema setup SQL statements for Data Pipelines Project 4.
#

# DROP TABLES

staging_events_table_drop =  "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =   "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =        "DROP TABLE IF EXISTS songplays"
user_table_drop =            "DROP TABLE IF EXISTS users"
song_table_drop =            "DROP TABLE IF EXISTS songs"
artist_table_drop =          "DROP TABLE IF EXISTS artists"
time_table_drop =            "DROP TABLE IF EXISTS time"   


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(256),
    auth VARCHAR(256),
    firstName VARCHAR(256),
    gender VARCHAR(256),
    itemInSession INT,
    lastName VARCHAR(256),
    length NUMERIC,
    level VARCHAR(256),
    location VARCHAR(256),
    method VARCHAR(256),
    page VARCHAR(256),
    registration BIGINT,   --VARCHAR(256), 
    sessionId INT,
    song VARCHAR(256),
    status VARCHAR(256),
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INT)  
DISTSTYLE EVEN
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INT,
    artist_id VARCHAR(256),
    artist_latitude NUMERIC(20,6), 
    artist_longitude NUMERIC(20,6),
    artist_location VARCHAR(512),
    artist_name VARCHAR(512),
    song_id VARCHAR(256),
    title VARCHAR(512),
    duration NUMERIC(12,2),
    year INT) 
DISTSTYLE EVEN
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(  
    songplay_id VARCHAR(32),  
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(256),
    song_id VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR(256),
    user_agent VARCHAR(256),
    PRIMARY KEY (songplay_id)
)
DISTSTYLE EVEN
SORTKEY (songplay_id, song_id, artist_id)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    gender VARCHAR(256) NOT NULL,
    level VARCHAR(256) NOT NULL)  
DISTSTYLE ALL
SORTKEY (user_id)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(512) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    year INT,
    duration NUMERIC(12,2)
)
DISTSTYLE ALL
SORTKEY (song_id)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    location VARCHAR(512),
    latitude NUMERIC(20,6),
    longitude NUMERIC(20,6)) 
DISTSTYLE ALL
SORTKEY (artist_id)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL) 
DISTSTYLE ALL
SORTKEY (start_time)
""")

# TABLE PROCESSING LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]

drop_table_queries =   [staging_events_table_drop, staging_songs_table_drop, 
                        songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop, time_table_drop]
