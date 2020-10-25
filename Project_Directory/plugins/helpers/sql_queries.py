class SqlQueries:
      
    us_cities_table_insert = ("""
        INSERT INTO us_cities (city, state) (
        SELECT DISTINCT city, region 
        FROM staging_world_cities 
        WHERE country='us')
    """)
    
    join_listings_tables = ("""
    SELECT p1.job_id, field1, index_, job_title, salary_estimate,
	   job_description, rating, company_name, location, headquarters, size_,
       founded, type_of_ownership, industry, sector, revenue, competitors, easy_apply
    FROM staging_job_listings_part_1 AS p1 
    JOIN staging_job_listings_part_2 AS p2 ON p1.job_id = p2.job_id
    JOIN staging_job_listings_part_3 AS p3 ON p1.job_id = p3.job_id 
    ORDER BY p1.job_id ASC
    """)
    
    companies_insert = ("""
        INSERT INTO companies (
        company_id, company_name, year_founded, headquarters,
        size_, revenue, industry, sector
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    job_titles_insert = ("""
        INSERT INTO job_titles (job_id, title, description)
        VALUES (%s, %s, %s)
        """)
    
    job_requirement_insert_part1 = ("""
        INSERT INTO data_job_requirements 
               (job_id, company_id, company_name, job_title, city, state, salary_estimate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
    
    select_data_tools = ("""
    SELECT tool_keyword, tool_name FROM data_tools
    ORDER BY tool_keyword
    """)
    
    retrieve_descriptions = ("""
    SELECT job_id, description FROM job_titles
    """)

    