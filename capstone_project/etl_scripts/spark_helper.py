
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .appName("capstone-etl") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark