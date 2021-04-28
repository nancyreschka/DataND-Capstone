import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnan, when, count
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.functions import from_unixtime, to_date, unix_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, LongType, TimestampType
import etl_capstone 


def create_spark_session():
    """
    - Creates a spark session with spark and SAS data source.
    
    Parameters:
        None

    Returns:
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport(). \
        getOrCreate()
    
    return spark

def main():
    """
    - Creates spark session
    - Processes the label descriptions for immigration data
    - Processes the immigration data
    - Processes the demographic data
    - Processes the world temperature data
    
    Parameters:
        None

    Returns:
        None
    """
    spark = create_spark_session()

    # input data paths
    labels_file_name = './I94_SAS_Labels_Descriptions.SAS'
    immigration_data_file_name = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    demographic_data_file_name = "us-cities-demographics.csv"
    world_temperature_data_file_name = '../../data2/GlobalLandTemperaturesByCity.csv'
    
    # output data
    output_data = 'analysis_data'
    
    # read label descriptions for immigration data
    i94cit_res_df, i94port_df, i94mode_df, i94state_df, i94visa_df = etl_capstone.read_i94_sas_label_descriptions(spark, labels_file_name)
    
    # read immigration data
    immi_df = etl_capstone.read_and_clean_immigration_data(spark, immigration_data_file_name)
    
    # read demographic data
    demo_df = etl_capstone.read_and_clean_demographic_data(spark, demographic_data_file_name)
    
    # read world temperature data
    temp_df = etl_capstone.read_and_clean_temperature_data(spark, world_temperature_data_file_name)
    temp_df = etl_capstone.join_temperature_data_with_country_codes(temp_df, i94cit_res_df)
    
    # create view for data quality checks
    i94cit_res_df.createOrReplaceTempView("country_codes")
    i94port_df.createOrReplaceTempView("port_codes")
    i94mode_df.createOrReplaceTempView("mode_codes")
    i94state_df.createOrReplaceTempView("state_codes")
    i94visa_df.createOrReplaceTempView("visa_codes")
    immi_df.createOrReplaceTempView("immigration")
    demo_df.createOrReplaceTempView("demographic")
    temp_df.createOrReplaceTempView("temperature")

    # data checks
    data_checks = {"country_codes": i94cit_res_df,
                   "port_codes": i94port_df,
                   "mode_codes": i94mode_df,
                   "state_codes": i94state_df,
                   "visa_codes": i94visa_df,
                   "immigration": immi_df,
                   "demographic": demo_df,
                   "temperature": temp_df }
    
    columns_to_check = {"country_codes": ["country_code"],
                   "port_codes": ["port_code"],
                   "mode_codes": ["mode_code"],
                   "state_codes": ["state_code"],
                   "visa_codes": ["visa_code"],
                   "immigration": ["cicid", "year", "month", "citizenship", "residence", "state_code"],
                   "demographic": ["id", "city", "state_code"],
                   "temperature": ["temp_id", "country_code"] }

    for table_name, data_frame in data_checks.items():
        etl_capstone.quality_check(table_name, data_frame, 0)
        etl_capstone.nullValueCheck(spark, table_name, columns_to_check[table_name])
        
    # save data to parquet files
    i94cit_res_df.write.parquet(output_data + "/country_codes", mode="overwrite")
    i94port_df.write.parquet(output_data + "/port_codes", mode="overwrite")
    i94mode_df.write.parquet(output_data + "/mode_codes", mode="overwrite")
    i94state_df.write.parquet(output_data + "/state_codes", mode="overwrite")
    i94visa_df.write.parquet(output_data + "/visa_codes", mode="overwrite")
    immi_df.write.parquet(output_data + "/immigration", mode="overwrite")
    demo_df.write.parquet(output_data + "/demographic", mode="overwrite")
    temp_df.write.parquet(output_data + "/temperature", mode="overwrite")

if __name__ == "__main__":
    main()
