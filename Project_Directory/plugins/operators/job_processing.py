from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class JobProcessingOperator(BaseOperator):

    ui_color = '#80BD9E'

        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 insert_mode="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(JobProcessingOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.insert_mode=insert_mode
     
    
    def fix_copmany_name(self, name):
        # remove the rating from the end of the string
        # if the company name exists
        if len(name) > 0:
            ind = len(name) - 1
            while ind != -1 and (name[ind] == '.' or name[ind].isdigit()):
                name = name[0: ind]
                ind -= 1

        # remove the extra whitespace and return
        return name.strip()
        
    def split_location(self, item):
        # into city and state
        item = item.replace(" ", "")
        items = item.split(',')
        
        hasTwoChars = False
        locCity = ""
        locState = ""
        
        if len(items) == 1:
            if items[0] != '-1' and items[0] != "":
                self.log.info(f"No state in location: {item}") # test
                locCity = items[0]
        elif len(items) == 2:
            if len(items[1]) == 2:  # valid city and state
                hasTwoChars = True
                locCity = items[0]
                locState = items[1]
            else:
                self.log.info(f"State is longer than 2 characters: {item}") # test
                locCity = items[0]
        else:
            self.log.info(f"more than 2 parts for the location {item}") # test
            locCity = item  # more than two names. put them all in city to avoid errors
                
        return hasTwoChars, locCity, locState 

     
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()
        
        self.log.info(f"Joining the 3 listings tables")
        cursor.execute(SqlQueries.join_listings_tables)
        records = cursor.fetchall()
        self.log.info(f"length of joins {len(records)}") # test
                     
        
        self.log.info(f"shifting columns if needed")
               
        all_companies = []
        all_job_titles = []
        all_job_details = []  # initial details without the required tools
        
        company_id = 1000
        
        for x in range(len(records)):
#         for x in range(9000):  # test sample
            current_columns = []
            new_columns = []
            for y in range(18):
                current_columns.append(records[x][y])
                                             
            new_columns.append(current_columns[0]) # job_id
            
            if current_columns[1] and not current_columns[1].isdigit():
#                 self.log.info(f"field 1 is storing the job title instead, so we shift the columns")  
                new_columns.append(None) # null field1
                new_columns.append(None) # null index_                      
                                  
#             else:
#                 self.log.info(f"no need to shift columns")
                                       
            # append rest of data as is
            for z in range(1, 18):   
                if current_columns[z] == '-1':
                    new_columns.append(None)
                else:    
                    new_columns.append(current_columns[z]) 
            
#             self.log.info(f"{new_columns}")

            new_columns[7] = self.fix_copmany_name(new_columns[7])
    
            # only continue if the company name is not empty
            if (len(new_columns[7]) > 0):
    
                # split the location into city and state
                hasTwoChars, loc_city, loc_state = self.split_location(new_columns[8])


                # checking the city and state against our US cities table
                # if the city-state pair doesn't exist, we will log it before adding it

                # Comparison against the database ignores case sensitivity and whitespace
#                 us_cities_query = """SELECT * FROM us_cities 
#                 WHERE REPLACE(LOWER(city), ' ', '') = REPLACE(LOWER(%s), ' ', '')
#                 AND REPLACE(LOWER(state), ' ', '') = REPLACE(LOWER(%s), ' ', '') 
#                 """
#                 get_city = redshift_hook.get_records(us_cities_query, (loc_city, loc_state))            
#                 if len(get_city) < 1 or len(get_city[0]) < 1 or get_city[0][0] < 1:
#                     self.log.info(f"Location: [{new_columns[8]}] Not Found in us_cities Database")

                all_companies.append((company_id,
                                new_columns[7], # company_name
                                new_columns[11], # year_founded
                                new_columns[9], # headquarters
                                new_columns[10], # size_
                                new_columns[15], # revenue
                                new_columns[13], # industry
                                new_columns[14] # sector
                                ))  

                all_job_titles.append((new_columns[0], # job_id
                                               new_columns[3], # job_title
                                               new_columns[5]) # description
                        )                       

                all_job_details.append((new_columns[0], # job_id
                                                company_id,
                                                new_columns[7], # company_name
                                                new_columns[3], # job_title
                                                loc_city,
                                                loc_state,
                                                new_columns[4]  # salary estimate                             
                        ))


                company_id += 1
            else:
                self.log.info(f"Job ID: [{new_columns[0]}]. Company Name: [{new_columns[7]}] Not Added")
                    
        # insert values into companies and job_titles dimension tables
        cursor.executemany(SqlQueries.companies_insert, all_companies)
        connection.commit()
        cursor.executemany(SqlQueries.job_titles_insert, all_job_titles)
        connection.commit() 
        
        # insert initial values into data_job_requirements fact table (without the tools yet)
        cursor.executemany(SqlQueries.job_requirement_insert_part1, all_job_details)
        connection.commit()

