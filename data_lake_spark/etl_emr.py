import configparser
from datetime import datetime
import os
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def get_json_s3_paths(bucket_name, prefix_name):
    """
    Given a bucket name and prefix, get all S3 paths for JSON files

    This is done so that we can reduce the AWS ListAPI calls when performing spark.read()
    :param str bucket_name: Name of the S3 bucket
    :param str prefix_name: Name of the prefix to read
    :return: List of S3 paths containing JSON files
    :rtype: List[str]
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    s3_objects = []
    for obj in bucket.objects.filter(Prefix=prefix_name):
        s3_objects.append(obj)
    return [f"s3n://{obj.bucket_name}/{obj.key}" for obj in s3_objects if obj.key[-5:]=='.json']


def create_spark_session():
    """
    Creates and returns a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .appName("sparkify-etl") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark


def process_song_data(spark, input_bucket, output_bucket):
    """
    Processes song logs from S3 and creates song and artist dimension tables on S3

    :param SparkSession spark: SparkSession used for this ETL
    :param str input_bucket: S3 bucket name containing song logs
    :param str output_bucket: S3 bucket to write dimension tables to
    :return song_data: DataFrame containing song logs read from S3
    :rtype pyspark.sql.DataFrame
    """
    # Get all JSON S3 paths for song_data from input_bucket
    song_input_paths = get_json_s3_paths(bucket_name=input_bucket, prefix_name="song_data")
    # Create output S3 paths for song and artist dimension tables
    output_base_path = f"s3n://{output_bucket}"
    song_output_path = os.path.join(output_base_path, "song_dim")
    artist_output_path = os.path.join(output_base_path, "artist_dim")
    print("Reading song logs")
    song_data = spark.read.json(song_input_paths)
    print(song_data.printSchema())
    song_data.cache()
    print("Creating song_dim table")
    songs_table = song_data\
            .filter((f.col('song_id').isNotNull()) &
                    (f.col('title').isNotNull()) &
                    (f.col('artist_id').isNotNull()) &
                    (f.col('year').isNotNull()) &
                    (f.col('duration').isNotNull())
                   )\
            .dropDuplicates(subset=['song_id'])\
            .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songs_table.coalesce(16)
    print(songs_table.printSchema())
    print("Writing song_dim table")
    songs_table.coalesce(16).write.partitionBy(['year', 'artist_id']).mode("overwrite").parquet(song_output_path)

    print("Creating artist_dim table")
    artists_table  = song_data\
                .filter((f.col('artist_id').isNotNull()) &
                        (f.col('artist_name').isNotNull())
                       )\
                .dropDuplicates(subset=['artist_id'])\
                .select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                .withColumnRenamed('artist_name', 'name')\
                .withColumnRenamed('artist_location', 'location')\
                .withColumnRenamed('artist_latitude', 'latitude')\
                .withColumnRenamed('artist_longitude', 'longitude')
    artists_table.coalesce(16)
    print(artists_table.printSchema())
    print("Writing artist_dim table")
    artists_table.coalesce(16).write.mode("overwrite").parquet(artist_output_path)
    return song_data


def process_log_data(spark, input_bucket, output_bucket, song_data):
    """
    Processes log_data from S3 and song_data DataFrame to create user and time dimension, songplay_fact tables

    :param SparkSession spark: SparkSession used for this ETL
    :param str input_bucket: S3 bucket name containing event logs
    :param str output_bucket: S3 bucket name to write user,time dimension and songplay fact table
    :param pyspark.sql.DataFrame song_data: DataFrame containing song_logs
    :return Nothing (Writes tables to S3)
    :rtype None
    """
    # Get all log_data paths from S3 to read
    log_data_paths = get_json_s3_paths(bucket_name=input_bucket, prefix_name="log_data")
    # Create output S3 path for each table
    output_base_path = f"s3n://{output_bucket}"
    user_output_path = os.path.join(output_base_path, "user_dim")
    time_output_path = os.path.join(output_base_path, "time_dim")
    songplays_output_path = os.path.join(output_base_path, "songplay_fact")
    print("Reading log data")
    df = spark.read.json(log_data_paths)

    print("Filtering for NextSong")
    filtered_df = df.filter(f.col('page') == 'NextSong')

    print("Creating user_dim table")
    users_table = filtered_df\
                .filter((f.col('userId').isNotNull()) &
                        (f.col('firstName').isNotNull()) &
                        (f.col('lastName').isNotNull()) &
                        (f.col('gender').isNotNull()) &
                        (f.col('level').isNotNull()) &
                        (f.col('page') == 'NextSong')
                )\
                .dropDuplicates(subset=['userId'])\
                .select(
                    f.col('userId').cast(IntegerType()),
                    'firstName', 'lastName', 'gender', 'level', 'page')
    users_table = users_table\
                .withColumnRenamed('userId', 'user_id')\
                .withColumnRenamed('firstName', 'first_name')\
                .withColumnRenamed('lastName', 'last_name')

    users_table.coalesce(16)
    print("Writing user_dim table")
    users_table.coalesce(16).write.mode("overwrite").parquet(user_output_path)

    print("Creating time_dim")
    time_table = filtered_df\
                .select(f.to_timestamp(f.from_unixtime(f.col('ts')/1000)).alias('start_time'))\
                .filter(f.col('start_time').isNotNull())\
                .dropDuplicates(subset=['start_time'])\
                .withColumn('hour',f.hour('start_time'))\
                .withColumn('day', f.dayofyear('start_time'))\
                .withColumn('week', f.weekofyear('start_time'))\
                .withColumn('month', f.month('start_time'))\
                .withColumn('year', f.year('start_time'))\
                .withColumn('weekday', f.dayofweek('start_time'))

    time_table.coalesce(16)
    print("Writing time dim")
    time_table.coalesce(16).write.partitionBy(['year', 'month']).mode("overwrite").parquet(time_output_path)

    # Avoid reading song_data from S3 again as it's super slow, and use the song_data DF passed to this function
    print("Creating songplays_fact")
    log_data_part = filtered_df.select(f.monotonically_increasing_id().alias('songplay_id'),
                                f.to_timestamp(f.from_unixtime(f.col('ts')/1000)).alias('start_time'),
                                f.col('userId').cast(IntegerType()).alias('user_id'),
                                'artist',
                                'song',
                                'level',
                                f.col('sessionId').alias('session_id'),
                                'location',
                                f.col('userAgent').alias('user_agent')
                               )\
                            .withColumn('month', f.month('start_time'))\
                            .withColumn('year', f.year('start_time'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_data_part.join(song_data,
                                   (song_data.title==log_data_part.song) & (song_data.artist_name == log_data_part.artist)
                                  )\
                            .select(['songplay_id', 'start_time', 'user_id', 'level',
                                     'song_id', 'artist_id', 'session_id', 'location', 'user_agent', log_data_part.year, log_data_part.month])
    songplays_table.coalesce(32)
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_fact table")
    songplays_table.coalesce(32).write.partitionBy(['year', 'month']).mode("overwrite").parquet(songplays_output_path)


def main():
    spark = create_spark_session()
    print(spark)
    print(spark.sparkContext.uiWebUrl)
    input_bucket = "udacity-dend"
    output_bucket = "data-eng-nd/sparkify-datalake"

    start_time = time.time()
    song_data = process_song_data(spark, input_bucket, output_bucket)
    process_log_data(spark, input_bucket, output_bucket, song_data)
    end_time = time.time()
    print(f"Took {(end_time - start_time)/60} minutes to run the ETL")


if __name__ == "__main__":
    main()
