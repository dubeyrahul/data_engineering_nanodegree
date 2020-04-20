# Project Description:
In this project: `data_modeling_postgres` we develop an ETL process to load JSON logs generated from a streaming music service: Sparkify into a Postgres database, to provide analytical querying abilities for our users.

# Dataset:
The dataset that we load into the Postgres DB consists of logs about the songs and consumer's listening activity.

# Database
We extract relevant information from these logs and construct a Sparkify database: sparkifydb. It consists of the following tables:
- songs: contains facts about the songs in the Sparkify streaming service
- artists: contains facts about the artists whose songs are available
- users: contains facts about users who use the Sparkify service
- time: contains user activity timestamps and related time-derived columns
- songplays: contains rows that corresponds to user's listening activity

# ETL guidelines:
Below listed is the scripts and the function they perform:
- create_table.py: this script is responsible for connecting to DB, dropping tables if they exist, and creating tables
- sql_queries.py: this file contains all the SQL code needed to run the ETL process
- etl.py: this is the main ETL performing script. It connects to DB, processes logs, transforms data as needed and inserts them into the Postgres DB

Instructions to run python script:
1. cd into this directory: data_modeling_postgres
2. Run create_table.py script to drop and create tables
3. Run etl.py (it is necessary to run from this directory since data path is hard-coded in etl.py script)

### Setup:
To set up psql locally on Ubuntu:
1. Install postgres and psycopg2-binary
2. make sure that postgresql.conf points to the port number and hostname present in create_table.py script
3. you can verify 2 by doing a netstat check (netstat -na | grep postgres)
4. once you change port/host in the conf file stop and restart the postgres service using: `service postgres stop` and `service postgres start`

