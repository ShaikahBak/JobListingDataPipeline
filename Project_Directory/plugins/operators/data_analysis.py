from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class DataAnalysisOperator(BaseOperator):

    ui_color = '#80BD9E'

        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 insert_mode="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(DataAnalysisOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.insert_mode=insert_mode
      
     
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()
        
        self.log.info(f"Starting Data Analysis:")
        
        self.log.info("")
        self.log.info(f"1.Top 10 hiring companies (based on number of job listings):")       
        query1 = """
                    SELECT company_name, COUNT(*) AS number_of_listings 
                    FROM data_job_requirements 
                    GROUP BY company_name 
                    ORDER BY number_of_listings DESC 
                    LIMIT 10
                 """
        cursor.execute(query1)
        result = cursor.fetchall()
        
        for x in range(len(result)):
            self.log.info(f"\t{result[x][0]}: {result[x][1]} Listings") 
            
         
        self.log.info("")
        self.log.info(f"2.Top 10 job locations (cities / states) (based on number of job listings)")       
        query2 = """
                    SELECT city, state, COUNT(*) AS number_of_listings FROM data_job_requirements 
                    GROUP BY city, state 
                    ORDER BY number_of_listings DESC 
                    LIMIT 10
                 """
        cursor.execute(query2)
        result = cursor.fetchall()
        
        for x in range(len(result)):
            self.log.info(f"\t{result[x][0]}, {result[x][1]}: {result[x][2]} Listings")
           
        
        self.log.info("")
        self.log.info(f"3.Top 5 sectors offering data-related jobs")
        query3 = """
                    SELECT sector, COUNT(DISTINCT company_name) AS number_of_companies 
                    FROM companies
                    GROUP BY sector
                    ORDER BY number_of_companies DESC
                    LIMIT 5
                 """
        cursor.execute(query3)
        result = cursor.fetchall()
        
        for x in range(len(result)):
            self.log.info(f"\tThe {result[x][0]} Sector: {result[x][1]} Listings")
           
        
        self.log.info("")  
        self.log.info(f"4.Frequency of mentioning ‘Python’ as a preferred programming language, as opposed to ‘Java’, in data-related jobs")       
        query4 = """
                    SELECT COUNT(*) AS number_of_listings 
                    FROM data_job_requirements 
                    WHERE mentioned_tools LIKE '%Python%'
                 """
        cursor.execute(query4)
        result = cursor.fetchall()
        
        python_mentions = result[0][0]
        
        self.log.info(f"=> Python is mentioned in {python_mentions} job descriptions")
        
        query4 = """
                    SELECT COUNT(*) AS number_of_listings 
                    FROM data_job_requirements 
                    WHERE mentioned_tools LIKE '%Java%'
                 """
        cursor.execute(query4)
        result = cursor.fetchall()
        
        java_mentions = result[0][0]
        
        self.log.info(f"=> Java is mentioned in {java_mentions} job descriptions")
        self.log.info("")
        
        if python_mentions > java_mentions:
            self.log.info(f"Python is mentioned in {round(python_mentions/java_mentions, 2)} more job descriptions than Java")
        else:
            self.log.info(f"Java is mentioned in {round(java_mentions/python_mentions, 2)} more job descriptions than Python")
            
        
        self.log.info("")
        self.log.info(f"5.Frequency of mentioning Apache Airflow, Spark or Hadoop in a job description")       
        query5 = """
                    SELECT COUNT(*) AS number_of_listings 
                    FROM data_job_requirements 
                    WHERE mentioned_tools LIKE '%Airflow%' 
                    OR mentioned_tools LIKE '%Spark%'
                    OR mentioned_tools LIKE '%Hadoop%'
                 """
        cursor.execute(query5)
        result = cursor.fetchall()
        
        apache_mentions = result[0][0]
        
        self.log.info(f"=> {apache_mentions} job descriptions mention Apache Airflow, Spark and/or Hadoop")
        
        
        

