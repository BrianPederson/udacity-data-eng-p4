# stage_redshift.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/24/2020
# PURPOSE: Custom Airflow operator extracts from S3 (json) to Redshift staging table for Data Pipelines Project 4.
#
# Included methods:
#     __init__ - class initializer
#     execute - encapsulates functionality of custom operator
#

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_jsonpath="auto",
                 stg_mode="append",
                 #delimiter=",",
                 #ignore_headers=1,
                 *args, **kwargs):
        """
        __init__ - class initializer

        Parameters:
           redshift_conn_id - identifier referencing Airflow connection config data
           table - table to be loaded
           s3_bucket - AWS S3 bucket name for source data files
           s3_key - path and optional name of source data file (can optionally be templated using Airflow context variables execution date year/month)
           s3_jsonpath - path and name of JSONPath file used with event/log json files.
           stg_mode - mode to process target staging table can be 'replace', 'append', or 'skip'
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_jsonpath = s3_jsonpath
        self.stg_mode = stg_mode
        #self.delimiter = delimiter         # this option doesn't apply to json files
        #self.ignore_headers = ignore_headers


    def execute(self, context):
        """
        execute - encapsulates functionality of custom operator
        """
        self.log.info(f'StageToRedshiftOperator starting for table: {self.table} from bucket: {self.s3_bucket} with filepath/key: {self.s3_key} using JSONPath: {self.s3_jsonpath} and mode: {self.stg_mode}')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.stg_mode.lower() == 'skip':
            self.log.info(f'Skipping table {self.table}')
            return
        elif self.stg_mode.lower() == 'replace':
            self.log.info(f'Replacing (truncate-copy) table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        else:
            self.log.info(f'Appending to table {self.table}')

        if self.s3_jsonpath and self.s3_jsonpath.lower() != 'auto':
            s3_path_jsonpath = ("s3://{}/{}".format(self.s3_bucket, self.s3_jsonpath))
        else:
            s3_path_jsonpath = self.s3_jsonpath
        #self.log.info(f"raw JSONPath: {self.s3_jsonpath} - cooked JSONPath: {s3_path_jsonpath}")

        rendered_key = self.s3_key.format(**context)
        self.log.info(f"raw key: {self.s3_key} - cooked key: {rendered_key}")

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"s3_path: {s3_path}")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_path_jsonpath,
            #self.ignore_headers,
            #self.delimiter,
        )

        redshift.run(formatted_sql)

