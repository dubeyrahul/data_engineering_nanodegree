There are 4 tables:
1. demographics data per city: us-cities-demographics.csv
2. temperature data per city: GlobalLandTemperaturesByCity.csv
3. immigration data : sas_data (parquet) or data (sas format)
4. airport-codes_csv.csv: information about airports

extra:
unemployment data per metropolitan:
https://www.bls.gov/lau/#tables

analysis requirements:
analyze immigration patterns based on temperature
analyze visa types based on city/state/temperature
analyze immigration patterns based on cities or states
analyze immigration pattern/temperature pattern based on race
analyze rate of immigration with rate of unemployment

steps:
1. clean the immigration data by using the data dictionary file.
Extract the codes and save it in file, then read the codes, and enrich the immigration data so that it's joinable with others

2. model the tables based on all or a few analysis requirements
rough idea: immigration is used for fact and temperature/demographics/immigration info are used for dimensions

$SPARK_HOME/bin/spark-submit --packages saurfang:spark-sas7bdat:3.0.0-s_2.11 immigration_etl.py