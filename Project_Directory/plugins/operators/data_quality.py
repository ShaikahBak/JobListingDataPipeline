import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 null_col_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.null_col_checks = null_col_checks

        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        tbl_index = 0
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
            # checking for null columns
            if not len(self.null_col_checks[tbl_index]) == 0:
                for col in self.null_col_checks[tbl_index]:
                    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                    if len(records) < 1 or len(records[0]) < 1:
                        raise ValueError(f"NULL-value check failed on column {col}. Table {table} returned no results")
                    num_records = records[0][0]
                    if num_records < 1:
                        logging.info(f"NULL-value check on table {table} check passed. Column {col} contains no NULL values")
                    else:
                        raise ValueError(f"NULL-value check failed on column {col}, table {table}. NULL values exist")
                        
            tbl_index += 1
                