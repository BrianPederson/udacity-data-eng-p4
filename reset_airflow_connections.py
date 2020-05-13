# reset_airflow_connections.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/20/2020
# PURPOSE: utility script to set two connections in Airflow environment for Data Pipelines Project 4.
#
#

from airflow import settings
from airflow.models import Connection

aws_credentials = Connection(
        conn_id='aws_credentials',
        conn_type='aws',  # renders as 'Amazon Web Services'
        host='',
        login='XXXXXXXXXXXXXXXXXXXX',
        password='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
)

redshift = Connection(
        conn_id='redshift',
        conn_type='postgres',   # renders as 'Postgres'
        host='redshift-cluster.clrsh2v4cj6v.us-west-2.redshift.amazonaws.com',
        login='awsuser',
        password='XXXXXXXX',
        port='5439',
        schema='dev'
)

session = settings.Session()

# note these do NOT throw errors if the connections already exist
session.add(aws_credentials)
session.add(redshift)

session.commit()
