import os
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame
from spark_helper import create_spark_session
import time

BASE_DIR = 'data/18-83510-I94-Data-2016'

  

def read_i94_data(spark, dir_name):
    sas_filenames = []
    df_list = []
    for filename in os.listdir(dir_name):
        sas_filenames.append(os.path.join(dir_name, filename))
    for filename in sas_filenames:
        df = spark.read.format("com.github.saurfang.sas.spark").load(filename)
        df = df.select(['i94yr',
                        'i94mon', 
                        'i94res',
                        'i94port',
                        'i94bir',
                        'i94addr',
                        'i94visa',
                        'gender',
                        'visatype'])
        df_list.append(df)
    i94_df = reduce(DataFrame.unionByName, df_list)
    return i94_df.repartition(8)

def get_i94_port_mapping(filename):
    i94port_map = dict()
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip().split("=")
            code = eval(line[0]).strip()
            location = eval(line[1]).strip()
            location = location.split(',')
            if len(location) < 2:
                city, state = 'unknown', 'unknown'
            else:
                city, state = location[0].strip().lower(), location[1].strip().lower()
            i94port_map[code] = (city, state)
    return i94port_map

def get_i94_res_mapping(filename):
    i94res_map = dict()
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip().split("=")
            code = eval(line[0])
            location = eval(line[1]).strip()
            i94res_map[code] = location
    return i94res_map

def get_i94_addr_map(filename):
    i94addr_map = dict()
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip().split("=")
            code = eval(line[0]).strip()
            location = eval(line[1]).strip()
            i94addr_map[code] = location
    return i94addr_map
def transform_i94_data(i94_data):
    i94port_map = get_i94_port_mapping('i94port.txt')
    i94res_map = get_i94_res_mapping('i94res.txt')
    i94addr_map = get_i94_addr_map('i94addr.txt')

    @F.udf
    def state_code_to_name(state_code):
        return i94addr_map.get(state_code, "all other codes")
            
    @F.udf
    def source_code_to_name(source_code):
        return i94res_map.get(source_code, "invalid")

    @F.udf
    def destination_code_to_city(destination_code):
        if destination_code in i94port_map:
            return i94port_map[destination_code][0]
        return "unknown"  
        
    i94_data_transformed = i94_data.select(
        F.col('i94yr').cast(IntegerType()).alias('immigration_year'),
        F.col('i94mon').cast(IntegerType()).alias('immigration_month'),
        source_code_to_name(F.col('i94res')).alias('immigration_source'),
        destination_code_to_city(F.col('i94port')).alias('immigration_destination_port'),
        F.col('i94bir').cast(IntegerType()).alias('immigrant_age'),
        state_code_to_name(F.col('i94addr')).alias('immigration_state'),
        F.col('i94visa').alias('visa_code'),
        F.col('gender'),
        F.col('visatype').alias('visa_type')
    )
    return i94_data_transformed
    
def main():
    spark = create_spark_session()
    print(spark.sparkContext.uiWebUrl)
    start_time = time.time()
    print("Reading I94 data")
    i94_data = read_i94_data(spark, BASE_DIR)
    print("Transforming I94 data")
    i94_data_transformed = transform_i94_data(i94_data)
    print("Writing I94 data")
    i94_data_transformed.repartition(8).write.mode('overwrite').parquet('transformed_data/i94_transformed/')
    end_time = time.time()
    print(f"Took {(end_time - start_time)/60} minutes to run the ETL for immigration data")


if __name__ == "__main__":
    main()