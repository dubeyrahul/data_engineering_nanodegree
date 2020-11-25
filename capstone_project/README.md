This is the capstone project for Data Engineering Nanodegree on Udacity.

## Goal:
The goal of this project is to create clean and joinable datasets for data analysis and data science use-cases. The datasets we create will help analysts and scientists analyze immigration patterns in the US. The datasets will be joinable on city that the immigrant is entering into.

## Raw Data:

We will be using the following data sources in this project:
1. Immigration data into US from 2016 
2. Demographics data of US cities
3. Global temperature data

## Assessing the data:
The I-94 data has a lot of codes that have some kind of mapping and this mapping is located in `I94_SAS_Labels_Descriptions.SAS`. Presence of these codes make it a little bit difficult to analyze the I-94 data.

The global temperature data consists of data from all around the world and average temperature per month for last many years.
For our use-case, we filter this data to be just for US cities since we want to build transformed datasets to analyze immigration pattern in the US

Lastly, we use the demographics data of US cities. This data gives us a good idea of composition of the city. We add some columns per city to indicate population of different races in the city.

## ETL and Data Model
We will perform ETL on these data sources in order to make 3 datasets. The ETL scripts are located under the `etl_scripts` directory which write the data to the `transformed_data` directory. We create the following transformed tables:
1. Immigration-table with codes replaced by information mentioned in the SAS file
2. Global temperature data, aggregated per city and month
3. US demographics data, where we aggregate counts for different races in the city

These 3 datasets are joinable on city and can be analyzed on their own as well.

NOTE: It takes about 3 mins to run ETL on one year of immigration data, and a few seconds to run the other two ETL scripts.

## Analytic questions:
Some possible questions we explore in the Immigration_analysis notebook:
- Which city has most immigration in the US
- Which city has most students immigration
- Which city has most tourists immigration
- Which city has most business travelers
- What are the most diverse cities in the US
- Which month has most student travelers
- is there a correlation between diversity and number of student travelers

## Next steps:
How would Spark or Airflow be incorporated? 
- Airflow can be used to consume monthly immigration data and produce monthly updated datasets

Why did you choose the model you chose?
- I am interested in analyzing immigration pattern in different cities, particulary of incoming students

Clearly state the rationale for the choice of tools and technologies for the project.
- I used Spark to do all the ETL as it has good SQL-like capabilities and deals with large data efficiently

Propose how often the data should be updated and why.
- It should be updated monthly for reading new immigration data

Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x:
- We need to partition the data by year and month to handle the large scale immigration data
If the pipelines were run on a daily basis by 7am
- If the pipelines run daily on 7am then we need to partition immigration data by date so that we can effectively read/write parquet data
If the database needed to be accessed by 100+ people.
- We can create a Redshift DWH or S3 Datalake for querying purpose and scale the querying capabilities.

## Steps to run:
To run ETL, run the following from capstone_project directory:

```$SPARK_HOME/bin/spark-submit etl_scripts/temperature_etl.py```

```$SPARK_HOME/bin/spark-submit etl_scripts/demographics_etl.py```

```$SPARK_HOME/bin/spark-submit --packages saurfang:spark-sas7bdat:3.0.0-s_2.11 etl_scripts/immigration_etl.py```

