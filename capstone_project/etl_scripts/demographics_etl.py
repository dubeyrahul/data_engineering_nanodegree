from pyspark.sql import functions as F
from spark_helper import create_spark_session
import time
RACE_COLUMN_NAMES = [
    'Black or African-American',
    'Hispanic or Latino',
    'White',
    'Asian',
    'American Indian and Alaska Native'
]
def load_demographics_data(spark):
    """
    Read and return US cities demographics data into a spark dataframe
    
    :param spark: Spark Session
    """
    demographics_df = spark.read.load('us-cities-demographics.csv', 
                                 format="csv", sep=";", inferSchema="true", header="true")
    demographics_df = demographics_df.select(
        F.lower(F.col('City')).alias('city'),
        F.col('Median Age').alias('median_age'),
        F.col('Male Population').alias('male_population'),
        F.col('Female Population').alias('female_population'),
        F.col('Total Population').alias('total_population'),
        F.col('Foreign-born').alias('foreign_born'),
        F.col('Average Household Size').alias('houshold_size'),
        F.col('State Code').alias('state'),
        F.col('Race').alias('race'),
        F.col('Count').alias('count_race')
    )
    return demographics_df

def pivot_by_race(demographics_df):
    """
    Pivot race counts into their own columns

    :param us_temperature_df: Spark DF containing US temperature data
    """
    demographics_pivot = demographics_df\
        .groupby(
            ['city',
            'median_age',
            'male_population',
            'female_population',
            'total_population',
            'foreign_born',
            'houshold_size',
            'state'])\
        .pivot('race', RACE_COLUMN_NAMES).sum('count_race')
    demographics_pivot = demographics_pivot\
        .withColumnRenamed('Black or African-American', 'african_american')\
        .withColumnRenamed('Hispanic or Latino', 'hispanic')\
        .withColumnRenamed('White', 'white')\
        .withColumnRenamed('Asian', 'asian')\
        .withColumnRenamed('American Indian and Alaska Native', 'native_american')
    return demographics_pivot

def main():
    spark = create_spark_session()
    print(spark.sparkContext.uiWebUrl)
    start_time = time.time()
    demographics_df = load_demographics_data(spark)
    demographics_race_agg_df = pivot_by_race(demographics_df)
    demographics_race_agg_df.repartition(1).write.mode('overwrite').parquet('transformed_data/demographics_transformed/')
    end_time = time.time()
    print(f"Took {(end_time - start_time)/60} minutes to run the ETL for demographics data")


if __name__ == "__main__":
    main()