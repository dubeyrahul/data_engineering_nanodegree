from pyspark.sql import functions as F
from spark_helper import create_spark_session
import time

@F.udf
def date_to_month(date_string):
    """
    UDF to get month number from yyyy-mm-dd date string
    """
    month = date_string.split("-")[1]
    return int(month)

def load_us_temperature_data(spark):
    """
    Read and return temperature by city data for US into a spark dataframe
    
    :param spark: Spark Session
    """
    temperature_df = spark.read.option("header",True).csv("GlobalLandTemperaturesByCity.csv")
    us_temperature_df = temperature_df.filter(F.col('Country') == 'United States')
    return us_temperature_df

def compute_monthly_aggregate(us_temperature_df):
    """
    Compute avg temperature per city per month using historical data

    :param us_temperature_df: Spark DF containing US temperature data
    """
    us_temperature_df = us_temperature_df.select(
        date_to_month(F.col('dt')).alias('month'),
        F.lower(F.col('City')).alias('city'),
        F.col('AverageTemperature').alias('avg_temperature'),
    )
    us_temperature_agg = us_temperature_df\
        .groupby(['city', 'month'])\
        .agg(F.round(F.avg('avg_temperature'),2).alias('avg_temperature'))
    return us_temperature_agg

def main():
    spark = create_spark_session()
    print(spark.sparkContext.uiWebUrl)
    start_time = time.time()
    us_temperature_data = load_us_temperature_data(spark)
    us_temperature_agg_data = compute_monthly_aggregate(us_temperature_data)
    us_temperature_agg_data.repartition(1).write.mode('overwrite').parquet('transformed_data/temperature_transformed/')
    end_time = time.time()
    print(f"Took {(end_time - start_time)/60} minutes to run the ETL for temperature data")


if __name__ == "__main__":
    main()