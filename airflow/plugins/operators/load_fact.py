# load_fact.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/24/2020
# PURPOSE: Custom Airflow operator extracts from Redshift staging table and transforms/loads into Redshift DWH table for Data Pipelines Project 4.
#
# Included methods:
#     __init__ - class initializer
#     execute- encapsulates functionality of custom operator
#

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 dwh_mode="append",
                 *args, **kwargs):
        """
        __init__ - class initializer

        Parameters:
           redshift_conn_id - identifier referencing Airflow connection config data
           table - table to be loaded
           load_sql - SQL used to load table
           dwh_mode - mode to process target DWH table can be 'replace', 'append', or 'skip' 
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql
        self.dwh_mode = dwh_mode

    def execute(self, context):
        """
        execute - encapsulates functionality of custom operator
        """  
        self.log.info(f'LoadFactOperator starting for table: {self.table} in mode: {self.dwh_mode}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.dwh_mode.lower() == 'skip':
            self.log.info(f'Skipping table {self.table}')
            return
        elif self.dwh_mode.lower() == 'replace':
            self.log.info(f'Replacing (truncate-insert) table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        else:
            self.log.info(f'Appending to table {self.table}')          

        redshift.run(self.load_sql)