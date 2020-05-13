# udac_dag.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/24/2020
# PURPOSE: Airflow DAG performs ELT from s3 json files to Redshift staging tables to Redshift DWH tables for Data Pipelines Project 4.
#
# Included functions:
#     none
# Parameters (implemented as Airflow variables)
#     1) air_start_date        Airflow start_date parameter    (e.g. '2018-11-01')
#     2) air_end_date          Airflow end_date parameter      (optional)
#     3) s3_bucket             S3 bucket                       (e.g. 'udacity-dend')
#     4) s3_log_jsonpath       S3 JSONPath for events/staging_events data (e.g. 'log_json_path.json')
#     5) s3_log_prefix         S3 prefix (subordinate directory pattern) for events data files (e.g. 'log-data/{execution_date.year}/{execution_date.month}/ ')
#     6) s3_song_prefix        S3 prefix (subordinate directory pattern) for songs data files  (e.g. 'song-data/')
#     7) stg_mode_events       Mode for processing staging events_staging table  (append, replace, skip)
#     8) stg_mode_songs        Mode for processing staging songs_staging table   (append, replace, skip)
#     9) dwh_mode_songplays    Mode for processing DWH songplays songplays table (append, replace, skip)
#    10) dwh_mode_songs        Mode for processing DWH songplays songs table     (append, replace, skip)
#    11) dwh_mode_artists      Mode for processing DWH songplays artists table   (append, replace, skip)
#    12) dwh_mode_users        Mode for processing DWH songplays users table     (append, replace, skip)
#    13) dwh_mode_time         Mode for processing DWH songplays timetable       (append, replace, skip)
#

from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# process Airflow session variables
# Note: These variables seem to be a cumbersome, inflexible way to parameterize/configure this process.
#       From what I can tell these session bound variables can not easily be changed or dropped via script/code.
#       It seems to force updates/deletes to be done via UI. Only add function can be scripted.
#       If this is true (after further research) then it would seem that other methods should be used to
#       parameterize/configure the process (i.e. config file or parameters in a database table).

logging.info(f'Parameter s3_bucket: {Variable.get("s3_bucket")}')
logging.info(f'Parameter s3_song_prefix: {Variable.get("s3_song_prefix")}')
logging.info(f'Parameter s3_log_prefix: {Variable.get("s3_log_prefix")}')
logging.info(f'Parameter s3_log_jsonpath: {Variable.get("s3_log_jsonpath")}')

try:
    start_date_raw = Variable.get("air_start_date")
except:
    raise ValueError(f"Input value for parameter start_date is missing or improperly formatted.")
(start_date_year, start_date_month, start_date_day) = start_date_raw.split('-')
logging.info(f'Parameter start_date: {start_date_raw}')

try:
    end_date_raw = Variable.get("air_end_date")
except:
    end_date_raw = '2110-12-31'     # reasonable end of time
(end_date_year, end_date_month, end_date_day) = start_date_raw.split('-')
logging.info(f'Parameter end_date: {end_date_raw}')

# Define keys and default values for file processing mode parameters for staging and DWH target tables
# Mode parameters can have value 'append' or 'replace' or 'skip'. Default is 'append'.
mode = {"stg_mode_events":    "append",
        "stg_mode_songs":     "append",
        "dwh_mode_songplays": "append",
        "dwh_mode_songs":     "append",
        "dwh_mode_artists":   "append",
        "dwh_mode_users":     "append",
        "dwh_mode_time":      "append", }

for key in mode:
    try:
        mode[key] = Variable.get(key)
    except:
        pass   # if parameter is missing or malformed then use the default above
    logging.info(f'Parameter {key}: value: {mode[key]}')
    if mode[key].lower() not in ('append', 'replace', 'skip'):
        raise ValueError(f"Input value for parameter {key}: {mode[key]} is unrecognized. Should be one of 'append', 'replace', 'skip'.")

default_args = {
    'owner': 'bpederso',
    'depends_on_past': False,
    'retries': 1,                          # s/b 3
    'retry_delay': timedelta(minutes=1),   # s/b 5
    'catchup': False,
    'email': ['brampiet@yahoo.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'schedule_interval': '@hourly'
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load data from S3 into Redshift staging and then transform data to DWH using Airflow',
          start_date=datetime(int(start_date_year), int(start_date_month), int(start_date_day)),
          end_date=datetime(int(end_date_year), int(end_date_month), int(end_date_day)),
          schedule_interval='@daily',               # originally was set hourly but why? song data is chunked on a daily basis...
          max_active_runs=1                         # when backfilling only run one at a time
         )


start_task = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get("s3_bucket"),
    s3_key=Variable.get("s3_log_prefix"),   # "log-data/2018/11/2018-11-30-events.json" -or- "log-data/{execution_date.year}/{execution_date.month}/"
    s3_jsonpath=Variable.get("s3_log_jsonpath"),
    stg_mode=mode["stg_mode_events"],
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get("s3_bucket"),
    s3_key=Variable.get("s3_song_prefix"),   # "song-data/A/A/A/" -or- "song-data/A/" -or- "song-data"
    s3_jsonpath="auto",                      # "log_json_path.json"
    stg_mode=mode["stg_mode_songs"],
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    load_sql=SqlQueries.songplays_table_insert,
    dwh_mode=mode["dwh_mode_songplays"],
)

run_quality_check_songplays = DataQualityOperator(
    task_id='Run_data_quality_check_songplays',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
)

load_users_dim_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    load_sql=SqlQueries.users_table_insert,
    dwh_mode=mode["dwh_mode_songplays"],
)

run_quality_check_users = DataQualityOperator(
    task_id='Run_data_quality_check_users',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
)

load_songs_dim_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    load_sql=SqlQueries.songs_table_insert,
    dwh_mode=mode["dwh_mode_songs"],
)

run_quality_check_songs = DataQualityOperator(
    task_id='Run_data_quality_check_songs',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
)

load_artists_dim_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    load_sql=SqlQueries.artists_table_insert,
    dwh_mode=mode["dwh_mode_artists"],
)

run_quality_check_artists = DataQualityOperator(
    task_id='Run_data_quality_check_artists',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
)

load_time_dim_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    load_sql=SqlQueries.time_table_insert,
    dwh_mode=mode["dwh_mode_time"],
)

run_quality_check_time = DataQualityOperator(
    task_id='Run_data_quality_check_time',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
)

end_task = DummyOperator(task_id='Stop_execution',  dag=dag)


# set up DAG graph
start_task >> stage_events_to_redshift
start_task >> stage_songs_to_redshift

stage_events_to_redshift  >> load_songplays_fact_table
stage_songs_to_redshift   >> load_songplays_fact_table
stage_songs_to_redshift   >> load_songs_dim_table
stage_songs_to_redshift   >> load_artists_dim_table
stage_events_to_redshift  >> load_users_dim_table
stage_events_to_redshift  >> load_time_dim_table

load_songplays_fact_table >> run_quality_check_songplays
load_songs_dim_table      >> run_quality_check_songs
load_artists_dim_table    >> run_quality_check_artists
load_users_dim_table      >> run_quality_check_users
load_time_dim_table       >> run_quality_check_time

run_quality_check_songplays >> end_task
run_quality_check_songs     >> end_task
run_quality_check_artists   >> end_task
run_quality_check_users     >> end_task
run_quality_check_time      >> end_task
