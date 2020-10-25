import logging

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator,
                               DataQualityOperator, JobProcessingOperator, 
                               ToolExtractionOperator, DataAnalysisOperator)

from helpers import SqlQueries


default_args = {
    'owner': 'Shae Bak',
    'start_date': datetime(2020, 10, 1),
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3
}

dag = DAG('cap_1.9.57',  # 'Capstone_Project_DAG',
          default_args=default_args,
          description='Job Listing Analysis Pipeline',
          schedule_interval='@weekly',  # run once a week
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_Execution', dag=dag)

# create all tables before starting (if they do not exist already)
create_all_tables = PostgresOperator(
    task_id='Create_Tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_job_listings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Job_Listings_part_1',
    dag=dag,
    table="staging_job_listings_part_1",
    s3_bucket="dend-capstone-shaikahbakerman",
    s3_key="data/job_listings_json/part1",
    jsn='auto'
)

stage_job_listings2_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Job_Listings_part_2',
    dag=dag,
    table="staging_job_listings_part_2",
    s3_bucket="dend-capstone-shaikahbakerman",
    s3_key="data/job_listings_json/part2",
    jsn='auto'
)

stage_job_listings3_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Job_Listings_part_3',
    dag=dag,
    table="staging_job_listings_part_3",
    s3_bucket="dend-capstone-shaikahbakerman",
    s3_key="data/job_listings_json/part3",
    jsn='auto'
)

load_data_tool_db = StageToRedshiftOperator(
    task_id='Data_Tools',
    dag=dag,
    table="data_tools",
    s3_bucket="dend-capstone-shaikahbakerman",
    s3_key="data/data_tools/data_tools.csv",
    isCSV = True
)

stage_world_cities_to_redshift = StageToRedshiftOperator(
    task_id='Stage_World_Cities',
    dag=dag,
    table="staging_world_cities",
    s3_bucket="dend-capstone-shaikahbakerman",
    s3_key="data/world_cities/worldcitiespop.csv",
    isCSV = True
)

load_us_cities_dimension_table = LoadDimensionOperator(
    task_id='Load_us_cities_dim_table',
    dag=dag,
    table="us_cities",
    sql_insert_query=SqlQueries.us_cities_table_insert,
    insert_mode="truncate_insert"  # or just "insert"
)

# loading data into companies dimension table +
# loading data into job_titles dimension table +
# loading a portion of the data_job_requirements fact table columns
process_job_listings = JobProcessingOperator(
    task_id='process_job_listings',
    dag=dag,
    insert_mode="truncate_insert"  # or just "insert"
)

extract_data_tools = ToolExtractionOperator(
    task_id='extract_data_tools',
    dag=dag,
    insert_mode="truncate_insert"  # or just "insert"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["data_tools", "companies", "job_titles", "data_job_requirements"],
    # null column checks should include the columns that need checking against null values
    # each element in the list is a list that corresponds to a table from 'tables'
    null_col_checks=[["tool_id", "tool_keyword"], ["company_id", "company_name"], ["job_id", "job_title_id", "title"], 
                     ["job_id", "company_id"]]    
)

perform_data_analysis = DataAnalysisOperator(
    task_id='perform_data_analysis',
    dag=dag,
    insert_mode="truncate_insert"  # or just "insert"
)

end_operator = DummyOperator(task_id='End_Execution', dag=dag)


start_operator >> create_all_tables

create_all_tables >> stage_job_listings_to_redshift
create_all_tables >> stage_job_listings2_to_redshift
create_all_tables >> stage_job_listings3_to_redshift
create_all_tables >> load_data_tool_db
create_all_tables >> stage_world_cities_to_redshift
stage_world_cities_to_redshift >> load_us_cities_dimension_table

stage_job_listings_to_redshift >> process_job_listings
stage_job_listings2_to_redshift >> process_job_listings
stage_job_listings3_to_redshift >> process_job_listings
load_us_cities_dimension_table >> process_job_listings

load_data_tool_db >> extract_data_tools
process_job_listings >> extract_data_tools

extract_data_tools >> run_quality_checks
run_quality_checks >> perform_data_analysis
perform_data_analysis >> end_operator

