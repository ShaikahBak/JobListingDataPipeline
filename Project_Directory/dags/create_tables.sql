DROP TABLE IF EXISTS data_job_requirements;
DROP TABLE IF EXISTS job_titles;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS data_tools;
DROP TABLE IF EXISTS us_cities;
DROP TABLE IF EXISTS staging_world_cities;
DROP TABLE IF EXISTS staging_job_listings_part_3;
DROP TABLE IF EXISTS staging_job_listings_part_2;
DROP TABLE IF EXISTS staging_job_listings_part_1;


CREATE TABLE IF NOT EXISTS staging_job_listings_part_1 (
    job_id BIGINT PRIMARY KEY,
	field1 varchar(65535),
    index_ varchar(65535),
    job_title varchar(65535),
    salary_estimate varchar(65535)
);

CREATE TABLE IF NOT EXISTS staging_job_listings_part_2 (
    job_id BIGINT PRIMARY KEY,
    job_description varchar(65535)
);

CREATE TABLE IF NOT EXISTS staging_job_listings_part_3 (
    job_id BIGINT PRIMARY KEY,
    rating varchar(65535),
    company_name varchar(65535),
    location varchar(65535),
    headquarters varchar(65535),
    size_ varchar(65535),
    founded varchar(65535),
    type_of_ownership varchar(65535),
    industry varchar(65535),
    sector varchar(65535),
    revenue varchar(65535),
    competitors varchar(65535),
    easy_apply varchar(65535)
);

CREATE TABLE IF NOT EXISTS staging_world_cities (
    country varchar(256),
    city varchar(256),
    accent_city varchar(256),
    region varchar(256),
    population varchar(256),
    latitude varchar(256),
    longitude varchar(256)
);

CREATE TABLE IF NOT EXISTS us_cities (
    city_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city varchar(256) NOT NULL,
    state char(2) NOT NULL,
    UNIQUE (city, state)
);

CREATE TABLE IF NOT EXISTS data_tools (
    tool_id BIGINT PRIMARY KEY,
    tool_keyword varchar(256) NOT NULL UNIQUE,
    tool_name varchar(256)
);

CREATE TABLE IF NOT EXISTS companies (
    company_id BIGINT PRIMARY KEY,
    company_name varchar(65535) NOT NULL UNIQUE,
    year_founded INT,
    headquarters varchar(256),
    size_ varchar(256),
    revenue varchar(256),
    industry varchar(256),
    sector varchar(256)
);

CREATE TABLE IF NOT EXISTS job_titles (
	job_id BIGINT UNIQUE,
    job_title_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title varchar(256) NOT NULL,
    description varchar(65535)
);

CREATE TABLE IF NOT EXISTS data_job_requirements (
	job_id BIGINT PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(company_id),
    company_name varchar(256) NOT NULL REFERENCES companies(company_name),
    job_title varchar(256) NOT NULL,
    city varchar(256) NOT NULL,
    state char(2) NOT NULL,
    salary_estimate varchar(256),
    mentioned_tools varchar(256),
    FOREIGN KEY(city, state) REFERENCES us_cities(city, state)
);
