import configparser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sql_queries import *

import os

import re



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("capstone project") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_airport_data(spark, input_data, output_path):
    """
    Description: This function is responsible for reading airport data from the 
    project provided airport csv file, extracting latidude and longitude from the 
    cordinate column in to seperate columns and filtering out open US located airports.
    The formatted table is then writen to an s3  bucket.
    

    Arguments:
        spart: A spark session.
        input_data: path to raw aiport data.
        output_path: s3 repository

    Returns:
        None
    """
    
    df_airport = spark.read.csv(input_data, header = True)
    df_airport = df_airport.withColumn("longitude", F.split(df_airport.coordinates, ",").getItem(0)) \
                           .withColumn("latitude", F.split(df_airport.coordinates, ",").getItem(1))
    df_airport = df_airport.withColumn("state_code", F.split(df_airport.iso_region, "-").getItem(1))
    
    df_airport.createOrReplaceTempView("airport_table")
    airport_table = spark.sql(airport_tmp)
    
    airport_table.write.mode("overwrite").parquet(output_path + "/airport_dimension_data")

    
    
def process_city_data(spark, input_data_city, input_data_city_state_code, output_path):
    """
    Description: This function is responsible for reading city data from the 
    project provided US cities demographics and city code csv files. 
    Pivots the race columns to ensure a single row for each city, and include the city code column. 
    The formatted table is then writen to an s3  bucket.

    Arguments:
        spart: A spark session.
        input_data_city: path to raw US cities demographics data.
        input_data_city_state_code: path to US cities and state code (derived from data discription file)
        output_path: s3 repository

    Returns:
        None
    """
    
    #read in data files
    df_cities = spark.read.option("delimiter", ";").csv(input_data_city, header = True)
    df_city_state_code = spark.read.csv(input_data_city_state_code, header = True)
    
    #remove space and '-' from column names
    df_cities2 = df_cities.select([F.col(col).alias(col.replace(' ', '_')) for col in df_cities.columns])
    df_cities2 = df_cities2.select([F.col(col).alias(col.replace('-', '_')) for col in df_cities2.columns])
    
    #concat city name and state code
    df_cities2 = df_cities2.withColumn("city_state_code", F.concat_ws(('_'), F.col(("city")), F.col(("State_Code"))))
    df_city_state_code = df_city_state_code.withColumn("city_state_code", F.concat_ws(('_'), F.col(("city_name")), F.col(("state_code"))))
    
    #select and pivot race column from city data frame to get a single race row per city
    df_race = df_cities2.select(df_cities2.city_state_code, df_cities2.Race, df_cities2.Count.cast('int'))
    df_race = df_race.groupBy("city_state_code").pivot("Race").sum("Count")
    
    df_race = df_race.select([F.col(col).alias(col.replace(' ', '_')) for col in df_race.columns])
    df_race = df_race.select([F.col(col).alias(col.replace('-', '_')) for col in df_race.columns])
    
    
    #create temp table for all 3 data frames
    df_cities2.createOrReplaceTempView("cities")
    df_race.createOrReplaceTempView("race_table")
    df_city_state_code.createOrReplaceTempView("city_code")
    
    #join all 3 tables to get single row per city with complete city code
    city_state_code = spark.sql(city_tmp)
    
    city_state_code.write.mode("overwrite").parquet(output_path + "/city_dimension_data")
    



def fn_format_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None



def processed_immigration_data(spark, input_data, output_path):
    """
    Description: This function is responsible for reading immigration data from the 
    project provided immigrtion parque file.
    Change date columns to standard YYYYMMDD format.
    Format required columns to their appropriate data type so the match foreign keys in linked tables
    The processed table is then writen to an s3  bucket.
    
    Arguments:
        spart: A spark session.
        input_data: path to raw US immigration data.
        output_path: s3 repository

    Returns:
        None
    """
    
    df = spark.read.parquet(input_data)
    spark.udf.register("format_datetime", fn_format_datetime, T.DateType())
    df.createOrReplaceTempView("immigration_data")
    
    immigration_facts_table = spark.sql(immigration_tmp)
    
    immigration_facts_table.write.mode("overwrite").partitionBy("i94yr", "i94mon").parquet(output_path + "/immigration_facts_data")
    


#temperature was average per month of the year. i.e what is the average temp in jan, whats it in feb etc.
def process_avg_temp_data(spark, input_data, input_data_city_state_code, output_path):
    """
    Description: This function is responsible for reading weather data from the 
    project provided Global Land Temperatures By City csv files. 
    Computes the average temperature per calender month from 2010 to 2016 for each city.
    The formatted table is then writen to an s3  bucket.

    Arguments:
        spart: A spark session.
        input_data: path to raw Global Land Temperatures By City data.
        input_data_city_state_code: path to US cities and state code (derived from data discription file)
        output_path: s3 repository

    Returns:
        None
    """
    
    df_globe_temp = spark.read.csv(input_data, header = True)
    df_city_state_code = spark.read.csv(input_data_city_state_code, header = True)
    
    df_globe_temp.createOrReplaceTempView('temp_table')
    df_city_state_code.createOrReplaceTempView("city_code")
    avg_temp_table = spark.sql(avg_temp_tmp)
    
    avg_temp_table.write.mode("overwrite").partitionBy("month").parquet(output_path + "/us_temperature_dimension_data")    
    
    
def processed_visa_type(spark, input_data, output_path):
    df_visa = spark.read.csv(input_data, header = True)
    
    df_visa.write.mode("overwrite").parquet(output_path + "/visa_dimension_data")
        

def processed_transport_mode(spark, input_data, output_path):
    df_transport_mode = spark.read.csv(input_data, header = True)
    
    df_transport_mode.write.mode("overwrite").parquet(output_path + "/us_transport_mode_dimension_data")
    
    

def processed_country(spark, input_data, output_path):
    df_country_code = spark.read.csv(input_data, header = True)
    
    df_country_code.write.mode("overwrite").parquet(output_path + "/country_dimension_data")
    
    
#data qulity check
def data_copy_quality_check(spark, input_source, dest_path):
    """
    Description: To ensure data copy is complete, this function counts number of records
    in each source table and its coresponding destination table.
    The resultant dataframe (table_name, source_count, destination_count) is writen to an s3 bucket.
    The function also check for duplicate records in the processed city and airport tables.

    Arguments:
        spart: A spark session.
        input_source: path to all source data.
        dest_path: s3 repository

    Returns:
        None
    """
    
    #read source tables
    df_cities_source = spark.read.option("delimiter", ";").csv(input_source + "/us-cities-demographics.csv", header = True)
    df_cities_source = df_cities_source.select([F.col(col).alias(col.replace(' ', '_')) for col in df_cities_source.columns])
    df_visa_source = spark.read.csv(input_source + "/visa_type.csv", header = True)
    df_airport_source = spark.read.csv(input_source + "/airport-codes_csv.csv", header = True)
    #df_globe_temp_source = spark.read.csv(global_temp_path, header = True)
    df_country_code_source = spark.read.csv(input_source + "/country_code_processed.csv", header = True)
    df_transport_mode_source = spark.read.csv(input_source + "/transport_mode.csv", header = True)
    df_immigration_source = spark.read.parquet(input_source + "/sas_data")
    
    #create source temp tables
    df_cities_source = df_cities_source.createOrReplaceTempView('city_source_table')
    df_visa_source = df_visa_source.createOrReplaceTempView('visa_source_table')
    df_airport_source = df_airport_source.createOrReplaceTempView('airport_source_table')
    #df_globe_temp_source = df_globe_temp_source.createOrReplaceTempView('temp_source_table')
    df_country_code_source = df_country_code_source.createOrReplaceTempView('country_source_table')
    df_transport_mode_source = df_transport_mode_source.createOrReplaceTempView('transport_source_table')
    df_immigration_source = df_immigration_source.createOrReplaceTempView('immigration_source_table')
    
    
    #read destination tables
    df_cities_dest = spark.read.parquet(dest_path + "/city_dimension_data")
    df_visa_dest = spark.read.parquet(dest_path + "/visa_dimension_data")
    df_airport_dest = spark.read.parquet(dest_path + "/airport_dimension_data")
    #df_globe_temp_dest = spark.read.parquet(dest_path + "/us_temperature_dimension_data")
    df_country_code_dest = spark.read.parquet(dest_path + "/country_dimension_data")
    df_transport_mode_dest = spark.read.parquet(dest_path + "/transport_mode_dimension_data")
    df_immigration_dest = spark.read.parquet(dest_path + "/immigration_facts_data")
    
    
    #create destination temp tables
    df_cities_dest = df_cities_dest.createOrReplaceTempView('city_dest_table')
    df_visa_dest = df_visa_dest.createOrReplaceTempView('visa_dest_table')
    df_airport_dest = df_airport_dest.createOrReplaceTempView('airport_dest_table')
    #df_globe_temp_dest = df_globe_temp_dest.createOrReplaceTempView('temp_dest_table')
    df_country_code_dest = df_country_code_dest.createOrReplaceTempView('country_dest_table')
    df_transport_mode_dest = df_transport_mode_dest.createOrReplaceTempView('transport_dest_table')
    df_immigration_dest = df_immigration_dest.createOrReplaceTempView('immigration_dest_table')
    
    row_count_check = spark.sql(row_count_tmp)
    row_count_check.write.mode("overwrite").parquet(dest_path + "/row_count_check")
    
    duplicate_row_check = spark.sql(duplicate_record_check)
    duplicate_row_check.write.mode("overwrite").parquet(dest_path + "/duplicate_row_check")

    
    
def main():
    spark = create_spark_session()
    
    input_data_city = "source_data/us-cities-demographics.csv"
    input_data_city_state_code = "source_data/us_cities_processed.csv"
    airport_path = "source_data/airport-codes_csv.csv"
    global_temp_path = "../../data2/GlobalLandTemperaturesByCity.csv"
    country_code_path = "source_data/country_code_processed.csv"
    transport_mode_path = "source_data/transport_mode.csv"
    visa_type_path = "source_data/visa_type.csv"
    country_path = "source_data/country_code_processed.csv"
    immigration_path = 'source_data/sas_data'
    
    #input_data = "s3a://udacity-dend/"
    #output_path = "spark-warehouse"
    input_path = "source_data"
    output_path = "s3a://udacity-dend/output_tables"
    
    processed_visa_type(spark, visa_type_path, output_path)
    processed_transport_mode(spark, transport_mode_path, output_path)
    processed_country(spark, country_path, output_path)
    process_airport_data(spark, airport_path, output_path)
    process_city_data(spark, input_data_city, input_data_city_state_code, output_path)
    process_avg_temp_data(spark, global_temp_path, input_data_city_state_code, output_path)
    processed_immigration_data(spark, immigration_path, output_path)
    data_copy_quality_check(spark, input_path, output_path)
    



if __name__ == "__main__":
    main()















