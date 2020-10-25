from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

import re

class ToolExtractionOperator(BaseOperator):

    ui_color = '#80BD9E'

        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 insert_mode="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(ToolExtractionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.insert_mode=insert_mode
      
       
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()
        
        self.log.info(f"Retrieve the list of data tools")
        cursor.execute(SqlQueries.select_data_tools)
        tools = cursor.fetchall()
        
        # separate tool names from keywords
        tool_keywords, tool_names = zip(*tools)
        tool_keywords = list(tool_keywords)
        tool_names = list(tool_names)
        
        
        # retrieve job descriptions for tool extraction        
        self.log.info(f"Retrieve job description for tool extraction")
        cursor.execute(SqlQueries.retrieve_descriptions)
        descriptions = cursor.fetchall()
        
        # separate job_id from descriptions
        job_ids, descriptions = zip(*descriptions)
        job_ids = list(job_ids)
        descriptions = list(descriptions)

        
        # find the tools required in each job description
        self.log.info(f"Extracting data tools for each job and adding it to the fact table")
        
        for x in range(len(descriptions)):   
            description = descriptions[x].lower()
            
            tools_list = []
            for tool in tool_keywords:
                
                result = re.findall(r"[^a-zA-Z]"+re.escape(tool.lower())+"[^a-zA-Z]", description)
                if result:
                    tools_list.append(tool)
                    
            if tools_list:
                tools_list = ", ".join(tools_list)
            else:
                tools_list = None
            
            # update the data_job_requirements fact table           
            cursor.execute("UPDATE data_job_requirements SET mentioned_tools = %s WHERE job_id = %s", 
                           (tools_list, job_ids[x]))
            connection.commit()
            
            
                    
                                              
        
        
        
        
        
        
        



