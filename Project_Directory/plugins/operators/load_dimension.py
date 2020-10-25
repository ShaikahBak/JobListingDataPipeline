from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_insert_query="", 
                 insert_mode="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.insert_query = sql_insert_query
        self.insert_mode=insert_mode

        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        if self.insert_mode == "truncate_insert":
        
            self.log.info(f"Emptying the {self.table} table if it contains data")
            redshift_hook.run(f"TRUNCATE {self.table}")
        
        
        self.log.info(f"inserting data into the {self.table} table")
        redshift_hook.run(self.insert_query)
