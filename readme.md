## Project: Data Pipeline (4)
#### Data Engineering Nanodegree
##### Student: Brian Pederson
&nbsp;
#### Project Description
Using the fictitious startup Sparkify, build a dimensional star schema data model consisting of one fact and four dimensions utilizing Spark running on AWS. Write a basic ETL pipeline that transfers data from source json files stored in AWS S3 buckets using Python and SQL and then outputs the fact and dimensions as parquet files in AWS S3 buckets.

##### Data Sources (json files)
- song_data - s3://udacity-dend/song_data
- log_data - s3://udacity-dend/log_data

##### Data Targets (Redshift tables)
- songplays - fact table representing events associated with song plays
- users - dimension table representing users of the Sparkify service
- time - dimension table containing timestamps associated with songplay events
- songs - dimension table containing referenced songs
- artists - dimension table containing referenced artists

##### Program files
- airflow/dags/udac_dag.py - Airflow DAG to load data into Redshift DWH tables from S3 json files.
- airflow/plugins/__init__.py - Airflow Python boilerplate provided by instructor (no changes)
- airflow/plugins/helpers/sql_queries.py - python file containing SQL statements used by the DAG
- airflow/plugins/operators/data_quality.py - custom operator to check for rows in a table
- airflow/plugins/operators/stage_redshift.py - custom operator to load staging Redshift tables from S3 json files
- airflow/plugins/operators/load_fact.py - custom operator to load/transform Redshift fact tables from staging.
- airflow/plugins/operators/load_dimension.py - custom operator to load/transform Redshift dimension tables from staging.
- README.md - this descriptive file


Note the following files are part of the project but have been provided by the instructors and required no changes.
- airflow/plugins/__init__.py - Airflow Python boilerplate provided by instructor
- airflow/plugins/helpers/__init__.py - Airflow Python boilerplate provided by instructor
- airflow/pluginsoperators/__init__.py - Airflow python boilerplate provided by instructor

Note there are other files present in the project workspace used in development which are not technically part of the project submission but are used to set up the environment.
- air_kill.sh - script to kill Airflow server
- air_start.sh - script to start Airflow server
- dwh.cfg - configuration file containing details of the AWS Redshift environment (secret keys removed)
- cluster_create.py - python script to create AWS Redshift clluster
- create_tables.py - python utility program to create the staging and DWH tables in Redshift
- create_tables_helper.py - python file containing SQL statements used in create_tables.py
- dwh.cfg - configuration file containing details of the AWS Redshift environment
- reset_airflow_connections.py - script to reset Airflow connections "redshift" and "aws_credentials"
- reset_airflow_variables.py - script to set Airflow variables (used to configure Airflow DAG executions)

##### How to run the project
The process assumes :
- source data is located in an AWS s3 bucket with subdirectories song_data and log_data
- an IAM Role/ARN with approprirate read privs to the s3 data
- a Redshift cluster has been spun up and running with the appropriate software configuration.
- the dwh.cfg file has been edited to apply all necessary configuration parms including AWS keys
- the 2 staging, 1 DWH Fact and 4 DWH Dimension tables have been created using create_tables.py.
- Airflow is up and running and has the various DAG/operator files installed in proper directories
- Set up the Airflow connections for redshift and aws_credentials via Airflow UI
- Set up the Airflow variables which are used as parameters for the DAG. Use the Airflow UI or the python script reset_airflow_variables.py to initialize the variables; then use the Airflow UI to modify as needed. Note that this script can only be used to intialize the variables. It gives nasty errors if run after any of the variables are already set (either by the script or by UI).

The DAG is configured via Airflow Variables.
1) air_start_date       - Airflow start_date parameter    (e.g. '2018-11-01')
2) air_end_date         - Airflow end_date parameter      (optional)
3) s3_bucket            - S3 bucket                       (e.g. 'udacity-dend')
4) s3_log_jsonpath      - S3 JSONPath for events/staging_events data (e.g. 'log_json_path.json')
5) s3_log_prefix        - S3 prefix (subordinate directory pattern) for events data files (e.g. 'log-data/{execution_date.year}/{execution_date.month}/ ')
6) s3_song_prefix       - S3 prefix (subordinate directory pattern) for songs data files  (e.g. 'song-data/')
7) stg_mode_events      - Mode for processing staging events_staging table  (append, replace, skip)
8) stg_mode_songs       - Mode for processing staging songs_staging table   (append, replace, skip)
9) dwh_mode_songplays   - Mode for processing DWH songplays songplays table (append, replace, skip)
10) dwh_mode_songs      - Mode for processing DWH songplays songs table     (append, replace, skip)
11) dwh_mode_artists    - Mode for processing DWH songplays artists table   (append, replace, skip)
12) dwh_mode_users      - Mode for processing DWH songplays users table     (append, replace, skip)
13) dwh_mode_time       - Mode for processing DWH songplays timetable       (append, replace, skip)

Then run the DAG via the Airflow UI

##### Miscellaneous Notes
1. I utilized the tables I created for the Data Warehouse/Redshift project 2.
2. The three custom operators that load Redshift tables (staging, DWH Fact, DWH Dimension) all have the same load processing mode options. These are 'replace' which does a truncate prior to loading; 'append' which appends to existing data in table; and 'skip' which skips the processing step.


