from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


import json
import pandas as pd


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
      

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sql_query="",
                 jsn="",
                 isCSV = False,
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_query = sql_query
        self.jsn = jsn
        self.isCSV = isCSV
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        

    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        self.log.info(f"Emptying the {self.table} table if it contains data")
        redshift_hook.run(f"TRUNCATE {self.table}")
        

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,    
        )
        
        # add the file format to the COPY command
        if not self.isCSV:
            formatted_sql += f"json '{self.jsn}'" 
        else:
            formatted_sql += f"csv IGNOREHEADER 1"
        redshift_hook.run(formatted_sql)
