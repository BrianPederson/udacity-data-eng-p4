# sql_queries.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/28/2020
# PURPOSE: Script encapsulating insert/transform SQL statements for Data Pipeline Project 5.
#


class SqlQueries:

    songplays_table_insert = ("""
    INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT MD5(e.sessionid || e.ts) songplay_id,      -- make sure this converts the two numbers to strings before concatenating...
           TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 Second' AS start_time,
           e.userid as user_id,
           e.level,
           COALESCE(s.song_id, '***UNKNOWN_SONG***') as song_id,
           COALESCE(s.artist_id, '***UNKNOWN_ARTIST***') as artist_id,
           e.sessionid as session_id,
           e.location,
           e.useragent as user_agent
      FROM staging_events e
      LEFT OUTER JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
     WHERE page = 'NextSong'
       AND MD5(e.sessionid || e.ts) NOT IN (SELECT songplay_id FROM songplays)  -- no dups
    ;
    """)
    # Note: To properly deal with an incremental/append mode load of staging_events this should load songplays in two passes.
    #       1st pass should load songplays w/o join using the dummy FKs
    #       2nd pass should resolve the FKs using the songs and artists dimension tables (not staging_songs).
    #       Not doing it now since out of time... 

    users_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT userid, MIN(firstname), MIN(lastname), MIN(gender), MIN(level)
      FROM staging_events
     WHERE page = 'NextSong'
       AND userid NOT IN (SELECT user_id FROM users) -- no dups
     GROUP BY userid
    ;
    UPDATE users
       SET level = s.level,
           first_name = s.firstname,
           last_name = s.lastname,
           gender = s.gender
      FROM staging_events s
     WHERE user_id = s.userid   -- update existing rows with more recent info
    ;
    """)

    songs_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT song_id, MIN(title), MIN(artist_id), MIN(year), MIN(duration)
      FROM staging_songs
     WHERE song_id NOT IN (SELECT song_id FROM songs) -- no dups; retain original
     GROUP BY song_id
    UNION ALL
    SELECT '***UNKNOWN_SONG***', '***Unknown Song***', '***UNKNOWN_ARTIST***', 0, 0.0
     WHERE '***UNKNOWN_SONG***' NOT IN (SELECT song_id FROM songs)
    ;
    """)

    artists_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT artist_id, MIN(artist_name), MIN(artist_location), MIN(artist_latitude), MIN(artist_longitude)
      FROM staging_songs
     WHERE artist_id NOT IN (SELECT artist_id FROM artists) -- no dups; retain original
     GROUP BY artist_id
     UNION ALL
    SELECT '***UNKNOWN_ARTIST***', '*** Unknown Artist ***', '', 0, 0
     WHERE '***UNKNOWN_ARTIST***' NOT IN (SELECT artist_id FROM artists)
    ;
    """)

    time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
           EXTRACT(hour from start_time),
           EXTRACT(day from start_time),
           EXTRACT(week from start_time),
           EXTRACT(month from start_time),
           EXTRACT(year from start_time),
           EXTRACT(dayofweek from start_time)
      FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second' AS start_time
      FROM staging_events
     WHERE page = 'NextSong'
       AND start_time NOT IN (SELECT start_time FROM time) -- no dups; retain original
    );
    """)
