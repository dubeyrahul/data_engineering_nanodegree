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
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    s3_objects = []
    for obj in bucket.objects.filter(Prefix=prefix_name):
        s3_objects.append(obj)
    return [f"s3n://{obj.bucket_name}/{obj.key}" for obj in s3_objects if obj.key[-5:]=='.json']


def create_spark_session():
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
    # get filepath to song data
    song_input_paths = get_json_s3_paths(bucket_name=input_bucket, prefix_name="song_data")
#     song_input_path = os.path.join(input_data, "song_data/A/A/A/*.json")
    output_base_path = f"s3n://{output_bucket}"
    song_output_path = os.path.join(output_base_path, "song_dim")
    print("output-path:", song_output_path)
    artist_output_path = os.path.join(output_base_path, "artist_dim")
    # read song data file
    print("Reading song logs")
    song_data = spark.read.json(song_input_paths)
    print(song_data.printSchema())
    song_data.cache()

    # extract columns to create songs table
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
    # write songs table to parquet files partitioned by year and artist
    print(songs_table.printSchema())
    songs_table.coalesce(16).write.partitionBy(['year', 'artist_id']).mode("overwrite").parquet(song_output_path)
    print("Done writing songs_table")

    # extract columns to create artists table
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
    # write artists table to parquet files
    print(artists_table.printSchema())
    artists_table.coalesce(16).write.mode("overwrite").parquet(artist_output_path)
    print("Done writing artists_table")
    return song_data


def process_log_data(spark, input_bucket, output_bucket, song_data):
    # get filepath to log data file
    log_data_paths = get_json_s3_paths(bucket_name=input_bucket, prefix_name="log_data")
#     log_data_path = os.path.join(input_data, "log_data/*/*/*.json")
    output_base_path = f"s3n://{output_bucket}"
    user_output_path = os.path.join(output_base_path, "user_dim")
    time_output_path = os.path.join(output_base_path, "time_dim")
    songplays_output_path = os.path.join(output_base_path, "songplay_fact")
    # read log data file
    print("Reading log data")
    df = spark.read.json(log_data_paths)

    # filter by actions for song plays
    print("Filtering for NextSong")
    filtered_df = df.filter(f.col('page') == 'NextSong')

    # extract columns for users table
    print("Creating user dim")
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
    # write users table to parquet files
    print("Writing user dim")
    users_table.coalesce(16).write.mode("overwrite").parquet(user_output_path)

    # create timestamp column from original timestamp column
    print("Creating time dim")
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
    # write time table to parquet files partitioned by year and month
    print("Writing time dim")
    time_table.coalesce(16).write.partitionBy(['year', 'month']).mode("overwrite").parquet(time_output_path)

    # read in song data to use for songplays table
    # passed in song_data instead of reading it again
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
    print("Writing songplays_fact")
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
