# data_quality.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/26/2020
# PURPOSE: Custom Airflow operator performs simple data quality test ensuring target table has rows for Data Pipelines Project 4.
#
# Included methods:
#     __init__ - class initializer
#     execute - encapsulates functionality of custom operator
#

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):
        """
        __init__ - class initializer

        Parameters:
           redshift_conn_id - identifier referencing Airflow connection config data
           table - table to be "checked"
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id  

    def execute(self, context):
        """
        execute - encapsulates functionality of operator-task
        """  
        self.log.info(f'DataQualityOperator starting for table: {self.table}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        
        if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")  