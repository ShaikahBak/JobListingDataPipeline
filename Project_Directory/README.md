# Project Summary
In this project, I collected datasets of US data-related job listings, namely business
analyst, data engineer, data scientist and data analyst jobs, for the purpose of analyzing
these job listings, in order to find information about top companies and sectors that have
created data-related jobs, and to get some insight on most frequently-mentioned
technical skills and tools in the job descriptions.

I created an ETL process to stage data, test, clean and transform them, and then load
them into dimensional and fact tables following a Star schema. Later, I implemented
data quality checks to validate that all the tables are successfully loaded with data.
Finally, I executed a number of SQL queries to answer interesting questions about the
hiring companies, listed jobs and their required or preferred technical skills.

Datasets used in this project are data-related job listings, a database of world cities, and
a database of the top technical tools and software used in the data industry. This project
uses Amazon Web Services (AWS) S3 and Redshift for data storage and warehousing,
respectively. In addition, it uses Apache Airflow to schedule and monitor the data
pipeline.
