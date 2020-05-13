# reset_airflow_variables.py 
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/20/2020
# PURPOSE: utility script to set variables in Airflow environment for Data Pipelines Project 4.
#

from airflow import settings
from airflow.models import Variable

s3_bucket = Variable(key='s3_bucket', val='udacity-dend')
s3_log_jsonpath = Variable(key='s3_log_jsonpath', val='log_json_path.json')
#s3_song_prefix = Variable(key='s3_song_prefix', val='song-data')
#s3_log_prefix = Variable(key='s3_log_prefix', val='log-data')
s3_log_prefix = Variable(key='s3_log_prefix', val='log-data/{execution_date.year}/{execution_date.month}/')

# minimize data volume for dev/testing
s3_song_prefix = Variable(key='s3_song_prefix', val='song-data/B/')   # 'song-data/A/A/A/'
#s3_log_prefix = Variable(key='s3_log_prefix', val='log-data/2018/11/2018-11-30-events.json') # ditto

air_start_date = Variable(key='air_start_date', val="2018-11-30")
air_end_date   = Variable(key='air_end_date',   val="2018-11-30")

stg_mode_events    = Variable(key='stg_mode_events',    val="replace")
stg_mode_songs     = Variable(key='stg_mode_songs',     val="replace")
dwh_mode_songplays = Variable(key='dwh_mode_songplays', val="replace")
dwh_mode_songs     = Variable(key='dwh_mode_songs',     val="replace")
dwh_mode_artists   = Variable(key='dwh_mode_artists',   val="replace")
dwh_mode_users     = Variable(key='dwh_mode_users',     val="replace")
dwh_mode_time      = Variable(key='dwh_mode_time',      val="replace")

session = settings.Session()

# Note: These throw errors if the variables already exist (also variables are persistent across sessions)
# I can't find any method on Session that can delete or change a variable added to a session
# therefore have to delete these on the UI if trying to change within a session... ugh
session.add(s3_bucket)
session.add(s3_song_prefix)
session.add(s3_log_prefix)
session.add(s3_log_jsonpath)

session.add(stg_mode_events)
session.add(stg_mode_songs)
session.add(dwh_mode_songplays)
session.add(dwh_mode_songs)
session.add(dwh_mode_artists)
session.add(dwh_mode_users)
session.add(dwh_mode_time)

session.add(air_start_date)
session.add(air_end_date)

session.commit()

