import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnan, when, count, upper
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.functions import from_unixtime, to_date, unix_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, LongType, TimestampType

def convert_datetime(x):
    """
    - Converts a date string into datetime object in form yyyy-mm-dd.
    
    Parameters:
        x: date string

    Returns:
        datetime object with format yyyy-mm-dd
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

def read_and_clean_immigration_data(spark, immigration_data_file):
    """
    - Reads the immigration data
    - Removes the null values
    - Converts double to integer
    - Converts String to date in form yyyy-mm-dd
    - Changes column labels
    
    Parameters:
        spark: spark session
        immigration_data_file: path from where the data should be read

    Returns:
        immigration dataframe
    """
    print("read immigration data")
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_file)
    
    print("immigration data records:" + str(df_spark.count()))
    
    # remove null values
    df_spark = df_spark.na.fill(0, ["depdate", "i94bir", "biryear"])
    df_spark = df_spark.na.fill(99, ["i94addr"])
    df_spark = df_spark.na.fill(9, ["i94mode"])
    df_spark = df_spark.na.fill(999, ["i94cit", "i94res"])
    df_spark = df_spark.na.fill("", ["i94addr", "dtadfile", "visapost", "occup", "entdepa", "entdepd", "entdepu", "matflag", "dtaddto", "gender", "insnum", "airline", "fltno", "occup"])
    
    # cast double to integer
    df_spark = df_spark.withColumn("cicid", col("cicid").cast(IntegerType()))\
        .withColumn("i94yr", col("i94yr").cast(IntegerType()))\
        .withColumn("i94mon", col("i94mon").cast(IntegerType()))\
        .withColumn("i94cit", col("i94cit").cast(IntegerType()))\
        .withColumn("i94res", col("i94res").cast(IntegerType()))\
        .withColumn("i94mode", col("i94mode").cast(IntegerType()))\
        .withColumn("i94bir", col("i94bir").cast(IntegerType()))\
        .withColumn("i94visa", col("i94visa").cast(IntegerType()))\
        .withColumn("count", col("count").cast(IntegerType()))\
        .withColumn("biryear", col("biryear").cast(IntegerType()))
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())
    
    # convert String to date in form yyyy-mm-dd
    df_spark = df_spark.withColumn("arrdate", udf_datetime_from_sas("arrdate")) \
        .withColumn("depdate", udf_datetime_from_sas("depdate"))
    df_spark = df_spark.withColumn('dtadfile', to_date(unix_timestamp("dtadfile", 'yyyyMMdd').cast('timestamp')))
    df_spark = df_spark.withColumn('dtaddto', to_date(unix_timestamp("dtaddto", 'MMddyyyy').cast('timestamp')))
    
    # change column labels
    df_spark = df_spark.withColumnRenamed("i94yr", "year")\
        .withColumnRenamed("i94mon", "month")\
        .withColumnRenamed("i94cit", "citizenship")\
        .withColumnRenamed("i94res", "residence")\
        .withColumnRenamed("i94port", "port")\
        .withColumnRenamed("arrdate", "arrival_date")\
        .withColumnRenamed("i94mode", "mode")\
        .withColumnRenamed("i94addr", "state_code")\
        .withColumnRenamed("depdate", "departure_date")\
        .withColumnRenamed("i94bir", "age")\
        .withColumnRenamed("visacategory", "visa_category")\
        .withColumnRenamed("biryear", "birth_year")\
        .withColumnRenamed("visatype", "visa_type")
    
    print("immigration data records after cleaning:" + str(df_spark.count()))
    
    return df_spark

def code_mapper(f_content, idx):
    """
    - Parses the content of a file
    - Looks for idx
    - Translates the found positions into a dictionary
    
    Parameters:
        f_content: files content
        idx: string that shall be found

    Returns:
        dictionary of found entries
    """
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def read_i94_sas_label_descriptions(spark, labels_file_name):
    """
    - Reads the content of a file
    - Looks for i94cntl, i94prtl, i94model, i94addrl
    - Translates the found positions into a dataframe
    - generates the visa category information
    
    Parameters:
        spark: spark session
        labels_file_name: path where the file can be found

    Returns:
        dictionary of found entries
    """
    with open(labels_file_name) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
        
    print("read i94 label descriptions")
    
    # read i94 city and country codes
    i94cit_res = code_mapper(f_content, "i94cntyl")
    # convert dictionary in dataframe
    lol = list(map(list, i94cit_res.items()))
    i94cit_res_df = spark.createDataFrame(lol, ["country_code", "country_name"])
    # add column with unique id
    i94cit_res_df = i94cit_res_df.withColumn('id', monotonically_increasing_id())
    print("i94 country codes:" + str(i94cit_res_df.count()))
    
    # read i94 airport codes
    i94port = code_mapper(f_content, "i94prtl")
    # convert dictionary in dataframe
    lol = list(map(list, i94port.items()))
    i94port_df = spark.createDataFrame(lol, ["port_code", "port_name"])
    # add column with unique id
    i94port_df = i94port_df.withColumn('id', monotonically_increasing_id())
    print("i94 port codes:" + str(i94port_df.count()))
    
    # read i94 mode
    i94mode = code_mapper(f_content, "i94model")
    # convert dictionary in dataframe
    lol = list(map(list, i94mode.items()))
    i94mode_df = spark.createDataFrame(lol, ["mode_code", "mode_name"])
    # add column with unique id
    i94mode_df = i94mode_df.withColumn('id', monotonically_increasing_id())
    print("i94 mode codes:" + str(i94mode_df.count()))

    # read i94 addr
    i94addr = code_mapper(f_content, "i94addrl")
    # convert dictionary in dataframe
    lol = list(map(list, i94addr.items()))
    i94addrl_df = spark.createDataFrame(lol, ["state_code", "state_name"])
    # add column with unique id
    i94addrl_df = i94addrl_df.withColumn('id', monotonically_increasing_id())
    print("i94 addr codes:" + str(i94addrl_df.count()))

    # create i94 visa
    i94visa = {'1':'Business',
    '2': 'Pleasure',
    '3' : 'Student'}
    # convert dictionary in dataframe
    lol = list(map(list, i94visa.items()))
    i94visa_df = spark.createDataFrame(lol, ["visa_code", "visa_category"])
    # add column with unique id
    i94visa_df = i94visa_df.withColumn('id', monotonically_increasing_id())
    print("i94 visa codes:" + str(i94visa_df.count()))
    
    return i94cit_res_df, i94port_df, i94mode_df, i94addrl_df, i94visa_df
    
def read_and_clean_demographic_data(spark, demographic_data_file_name):
    """
    - Reads the demographic data
    - Drops rows with missing values
    - Changes column labels
    - Removes duplicate values
    - Adds column with unique id
    
    Parameters:
        spark: spark session
        demographic_data_file_name: path from where the data should be read

    Returns:
        demographic dataframe
    """
    print("read demographic data")
    demo_df = spark.read.options(header='True', inferSchema='True', delimiter=';').csv(demographic_data_file_name)
    
    print("demographic data records:" + str(demo_df.count()))
    
    # drop rows with missing values
    demo_df = demo_df.dropna(how='any', subset=["Male Population", "Female Population", "Number of Veterans", "Foreign-born", "Average Household Size"])
    
    # change column names
    demo_df = demo_df.withColumnRenamed("City", "city")\
        .withColumnRenamed("State", "state_name")\
        .withColumnRenamed("Median Age", "median_age")\
        .withColumnRenamed("Male Population", "male_population")\
        .withColumnRenamed("Female Population", "female_population")\
        .withColumnRenamed("Total Population", "total_population")\
        .withColumnRenamed("Number of Veterans", "number_of_veterans")\
        .withColumnRenamed("Foreign-born", "foreign_born")\
        .withColumnRenamed("Average Household Size", "average_household_size")\
        .withColumnRenamed("State Code", "state_code")\
        .withColumnRenamed("Race", "race")\
        .withColumnRenamed("Count", "count")
    
    # remove duplicate rows
    demo_df = demo_df.distinct()
    
    # add column with unique id
    demo_df = demo_df.withColumn('id', monotonically_increasing_id())
    
    print("demographic data records after cleaning:" + str(demo_df.count()))
    
    return demo_df

def read_and_clean_temperature_data(spark, temperature_data_file_name):
    """
    - Reads the temperature data
    - Drops rows with missing values
    - Changes column labels
    - Removes duplicate values
    - Adds column with unique id
    
    Parameters:
        spark: spark session
        temperature_data_file_name: path from where the data should be read

    Returns:
        temperature dataframe
    """
    print("read world temperature data")
    temp_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(temperature_data_file_name)
    
    print("world temperature data records:" + str(temp_df.count()))
    
    # drop rows with missing values
    temp_df = temp_df.dropna(how='any', subset=["AverageTemperature", "AverageTemperatureUncertainty"])
    
    # remove duplicate rows
    temp_df = temp_df.distinct()
    
    # change column names
    temp_df = temp_df.withColumnRenamed("dt", "date")\
        .withColumnRenamed("AverageTemperature", "average_temperature")\
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty")\
        .withColumnRenamed("City", "city")\
        .withColumnRenamed("Country", "country")\
        .withColumnRenamed("Latitude", "latitude")\
        .withColumnRenamed("Longitude", "longitude")
    
    # add column with unique id
    temp_df = temp_df.withColumn('temp_id', monotonically_increasing_id())
    
    print("world temperature data records after cleaning:" + str(temp_df.count()))
    
    return temp_df

def join_temperature_data_with_country_codes(temperature_data, country_codes):
    """
    - Joins world temperature data records with country codes
    - Selects the necessary columns
    
    Parameters:
        temperature_data: temperature dataframe
        country_codes: country codes dataframe

    Returns:
        temperature dataframe
    """
    print("join world temperature data records with country codes:" + str(temperature_data.count()))
    newtemp = temperature_data.join(country_codes, upper(temperature_data.country) == country_codes.country_name, "leftouter")
    newtemp = newtemp.select("temp_id", "date", "average_temperature", "average_temperature_uncertainty", "city", "country_name", "country_code", "latitude", "longitude")
    print("join world temperature data records with country codes after join:" + str(temperature_data.count()))
    return newtemp

def quality_check(table_name, data_frame):
    """
    - Checks if the dataframe has rows
    - If the dataframe doesn't have rows, the check failed, otherwise it passed.
    
    Parameters:
        table_name: name of the table thate will be checked
        data_frame: dataframe to be checked

    Returns:
        none
    """
    count = data_frame.count()
    
    if count == 0:
        print("Data quality check FAILED for table {}.".format(table_name))
    else:
        print("Data quality check PASSED for table {} with {} rows.".format(table_name, count))