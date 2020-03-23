## Project description:
In this project, we build an ETL notebook to read music event streams from csv files and design a no-sql schema using Cassandra to answer analytical queries on top of this events data.
The process looks like the following:
1. First we aggregate all the csv-part files
2. We analyze the queries that we want to model our data for
3. We create the tables to answer these queries efficiently

## Installation instructions:
### Step 1: Install Cassandra
Follow instructions at: http://cassandra.apache.org/doc/latest/getting_started/installing.html

### Step 2: Start Cassandra
Use `sudo service cassandra start` to start and `sudo service cassandra stop` to stop it. However, normally the service will start automatically. For this reason be sure to stop it if you need to make any configuration changes.
Check the port number on which Cassandra is running (verify that it is 9042) (hint: check this file: `/etc/cassandra/cassandra.yaml`

### Step 3: Install Python driver
```sh
$ pip3 install cassandra-driver
```
Or refer instructions at: https://docs.datastax.com/en/developer/python-driver/3.22/installation/

## Running instructions:
Open the notebook and run the cells. 
NOTE: the etl_helper.py contains util functions so as to simplify the look of the notebook and allow us to abstract some common code
